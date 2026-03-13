/**
 * @file
 * @brief `upgraded` 守护进程实现。
 *
 * 关键不变量：
 * - 目标 ABI 必须是小端序，且 `state.bin` 的磁盘布局必须无填充。
 * - 启动决策始终由 `state.bin` 驱动；`/appfs/current` 只是派生状态，不是事实来源。
 * - `state.bin` 落盘必须走 `tmp -> fdatasync(tmp) -> rename -> fsync(dir)`。
 * - `/appfs/current` 切换必须走 `symlink -> rename -> fsync(dir)`。
 * - 软件狗恢复顺序固定为：Level 1 重启进程，达到阈值后直接回滚槽位。
 *
 * 当前范围：
 * - 已实现：`boot_action` 消费、`bootcount` 自增、超限回滚、`current` 原子切换、
 *   软件狗 socket、硬件狗喂狗门控、进程重拉和直接回滚链路。
 * - 未实现：升级包下载/写槽、升级成功确认闭环，以及其他基于时间窗的高级抖动判据。
 * - 当前版本的回滚升级条件采用纯计数链路：Level 1 窗口计数直接触发回滚；
 *   不包含系统重启中间态，也不包含基于重启时间窗的抖动回滚策略。
 */

#include "apps/upgraded/upgraded.h"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

#include "external/include/ot_wtdg.h"
#include "libs/utils/log.hpp"
#include "upgraded/watchdog_protocol.h"

namespace upgraded {
namespace {

#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__)
#error "upgraded requires little-endian target for disk layout"
#endif

constexpr const char* kAppFsDir = "/appfs";
constexpr const char* kUpgradeDir = "/appfs/upgrade";
constexpr const char* kStatePath = "/appfs/upgrade/state.bin";
constexpr const char* kStateTmpPath = "/appfs/upgrade/state.bin.tmp";
constexpr const char* kStateBadPrefix = "/appfs/upgrade/state.bin.bad";
constexpr const char* kLockPath = "/appfs/upgrade/upgraded.lock";
constexpr const char* kCurrentPath = "/appfs/current";
constexpr const char* kCurrentTmpPath = "/appfs/current.new";
constexpr const char* kCurrentAppPath = "/appfs/current/app.bin";
constexpr const char* kSlotARelPath = "slots/A";
constexpr const char* kSlotBRelPath = "slots/B";
constexpr const char* kHardwareWatchdogPath = "/dev/watchdog";

constexpr uint8_t kMagic[4] = {'U', 'P', 'G', '1'};
constexpr uint16_t kStateVersion = 2;
constexpr uint32_t kDefaultBootLimit = 3;
constexpr uint8_t kPartA = 2;
constexpr uint8_t kPartB = 3;
constexpr uint8_t kBootActionNormal = 1;
constexpr uint8_t kBootActionRollback = 2;
constexpr uint32_t kFailReasonBootcountExceeded = 0x4001;

constexpr uint32_t kRestartBackoffInitMs = 200;
constexpr uint32_t kRestartBackoffMaxMs = 2000;
constexpr uint32_t kShutdownGraceMs = 2000;
constexpr uint32_t kPollMs = 100;

volatile sig_atomic_t g_stop_requested = 0;
volatile sig_atomic_t g_child_pid = -1;

#pragma pack(push, 1)
/**
 * @brief `state.bin` 的当前磁盘结构。
 * @note 当前仅支持结构版本 2；旧版 `state.bin` 会按非法状态文件处理并重建。
 * `reboot_count/reboot_limit` 字段当前保留未启用，后续若要恢复系统级策略可复用。
 */
struct StateDisk {
  uint8_t magic[4];
  uint16_t ver;
  uint16_t size;
  uint8_t upgrade_available;
  uint32_t bootcount;
  uint32_t bootlimit;
  uint8_t mender_boot_part;
  uint8_t boot_action;
  uint32_t last_fail_reason;
  uint32_t reboot_count;
  uint32_t reboot_limit;
  uint32_t reserved[2];
  uint32_t crc32;
};
#pragma pack(pop)

static_assert(sizeof(StateDisk) == 43u, "StateDisk size must remain stable");

struct WatchdogRuntime {
  std::mutex mu;
  int server_fd = -1;
  int client_fd = -1;
  int hw_fd = -1;
  pid_t active_pid = 0;
  int64_t child_start_ms = 0;
  int64_t last_valid_ping_ms = 0;
  int64_t healthy_since_ms = 0;
  bool healthy_confirmed = false;
  bool restart_requested = false;
  uint32_t restart_reason = 0;
  uint32_t last_bad_reason = 0;
};

struct ChildExitHealth {
  bool healthy_confirmed = false;
};

WatchdogRuntime g_watchdog;
std::mutex g_state_mu;
StateDisk g_state{};
std::mutex g_level1_mu;
std::deque<int64_t> g_level1_events;
std::atomic<bool> g_watchdog_thread_ready{false};
std::atomic<bool> g_watchdog_thread_failed{false};

int64_t NowMsSteady() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

const char* PartName(uint8_t part) {
  if (part == kPartA) return "A";
  if (part == kPartB) return "B";
  return "UNKNOWN";
}

const char* BootActionName(uint8_t action) {
  if (action == kBootActionNormal) return "NORMAL";
  if (action == kBootActionRollback) return "ROLLBACK";
  return "UNKNOWN";
}

void LogStateSnapshot(const char* tag, const StateDisk& state) {
  LOGI(
      "%s ver=%u size=%u upgrade_available=%u bootcount=%u bootlimit=%u "
      "part=%s(%u) boot_action=%s(%u) last_fail_reason=0x%08X "
      "reboot_count=%u reboot_limit=%u reserved=[0x%08X,0x%08X] crc32=0x%08X",
      tag, static_cast<unsigned>(state.ver), static_cast<unsigned>(state.size),
      static_cast<unsigned>(state.upgrade_available),
      static_cast<unsigned>(state.bootcount),
      static_cast<unsigned>(state.bootlimit), PartName(state.mender_boot_part),
      static_cast<unsigned>(state.mender_boot_part),
      BootActionName(state.boot_action),
      static_cast<unsigned>(state.boot_action),
      static_cast<unsigned>(state.last_fail_reason),
      static_cast<unsigned>(state.reboot_count),
      static_cast<unsigned>(state.reboot_limit),
      static_cast<unsigned>(state.reserved[0]),
      static_cast<unsigned>(state.reserved[1]),
      static_cast<unsigned>(state.crc32));
}

/**
 * @brief 处理守护进程的终止信号。
 * @param signum 内核投递的信号编号；当前逻辑只将其视为停止请求。
 */
void OnSignal(int signum) {
  (void)signum;
  g_stop_requested = 1;
  const pid_t pid = static_cast<pid_t>(g_child_pid);
  if (pid > 0) {
    (void)::kill(pid, SIGTERM);
  }
}

/**
 * @brief 安装 `SIGTERM` 和 `SIGINT` 处理器，用于受控退出。
 * @return 两个处理器都安装成功时返回 `true`，否则返回 `false`。
 */
bool InstallSignalHandlers() {
  struct sigaction sa {};
  sa.sa_handler = OnSignal;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  if (::sigaction(SIGTERM, &sa, nullptr) != 0) return false;
  if (::sigaction(SIGINT, &sa, nullptr) != 0) return false;
  return true;
}

/**
 * @brief 递归创建目录，效果类似 `mkdir -p`。
 * @param dir 要创建的绝对路径或相对路径。
 * @return 目录已存在或创建成功时返回 `true`。
 */
bool EnsureDirRecursive(const std::string& dir) {
  if (dir.empty()) return false;

  std::string current_path;
  current_path.reserve(dir.size());
  for (size_t i = 0; i < dir.size(); ++i) {
    current_path.push_back(dir[i]);
    if (dir[i] != '/' && i + 1 != dir.size()) continue;
    if (current_path == "/") continue;
    if (::mkdir(current_path.c_str(), 0755) != 0 && errno != EEXIST) {
      return false;
    }
  }
  return true;
}

/**
 * @brief 对目录执行 `fsync`，使目录项变更持久化。
 * @param dir 目录路径。
 * @return 成功时返回 `true`。
 */
bool FsyncDirectory(const char* dir) {
  const int dir_fd = ::open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (dir_fd < 0) {
    LOGE("open dir failed: %s path=%s", std::strerror(errno), dir);
    return false;
  }
  if (::fsync(dir_fd) != 0) {
    LOGE("fsync dir failed: %s path=%s", std::strerror(errno), dir);
    (void)::close(dir_fd);
    return false;
  }
  if (::close(dir_fd) != 0) {
    LOGW("close dir failed: %s path=%s", std::strerror(errno), dir);
  }
  return true;
}

/**
 * @brief 在无错误的前提下写满 `size` 字节。
 * @param fd 可写文件描述符。
 * @param data 源缓冲区。
 * @param size 需要写入的字节数。
 * @return 完整写入时返回 `true`，否则返回 `false`。
 */
bool WriteAll(int fd, const void* data, size_t size) {
  const uint8_t* bytes = static_cast<const uint8_t*>(data);
  size_t written = 0;
  while (written < size) {
    const ssize_t rc = ::write(fd, bytes + written, size - written);
    if (rc < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (rc == 0) {
      errno = EIO;
      return false;
    }
    written += static_cast<size_t>(rc);
  }
  return true;
}

/**
 * @brief 在无错误的前提下读取满 `size` 字节。
 * @param fd 可读文件描述符。
 * @param data 目标缓冲区。
 * @param size 需要读取的字节数。
 * @return 完整读取时返回 `true`，否则返回 `false`。
 */
bool ReadAll(int fd, void* data, size_t size) {
  uint8_t* bytes = static_cast<uint8_t*>(data);
  size_t consumed = 0;
  while (consumed < size) {
    const ssize_t rc = ::read(fd, bytes + consumed, size - consumed);
    if (rc < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (rc == 0) {
      errno = EIO;
      return false;
    }
    consumed += static_cast<size_t>(rc);
  }
  return true;
}

/**
 * @brief 计算 CRC-32/IEEE 802.3。
 * @param data 输入字节流。
 * @param len 缓冲区长度，单位为字节。
 * @return 采用输入/输出反射并执行最终异或后的 CRC-32 值。
 */
uint32_t ComputeCrc32Ieee(const uint8_t* data, size_t len) {
  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < len; ++i) {
    crc ^= static_cast<uint32_t>(data[i]);
    for (int bit = 0; bit < 8; ++bit) {
      if ((crc & 1u) != 0u) {
        crc = (crc >> 1u) ^ 0xEDB88320u;
      } else {
        crc >>= 1u;
      }
    }
  }
  return ~crc;
}

uint32_t CalculateStateCrc(const StateDisk& state) {
  return ComputeCrc32Ieee(reinterpret_cast<const uint8_t*>(&state),
                         sizeof(StateDisk) - sizeof(state.crc32));
}

StateDisk MakeDefaultState() {
  StateDisk state {};
  std::memcpy(state.magic, kMagic, sizeof(state.magic));
  state.ver = kStateVersion;
  state.size = static_cast<uint16_t>(sizeof(StateDisk));
  state.upgrade_available = 0;
  state.bootcount = 0;
  state.bootlimit = kDefaultBootLimit;
  state.mender_boot_part = kPartA;
  state.boot_action = kBootActionNormal;
  state.last_fail_reason = 0;
  state.reboot_count = 0;
  state.reboot_limit = 0;
  state.reserved[0] = 0;
  state.reserved[1] = 0;
  state.crc32 = CalculateStateCrc(state);
  return state;
}

bool ValidateCommonFields(uint8_t upgrade_available, uint8_t part, uint8_t action,
                          std::string* reason) {
  if (upgrade_available > 1) {
    *reason = "upgrade_available invalid";
    return false;
  }
  if (part != kPartA && part != kPartB) {
    *reason = "mender_boot_part invalid";
    return false;
  }
  if (action != kBootActionNormal && action != kBootActionRollback) {
    *reason = "boot_action invalid";
    return false;
  }
  return true;
}

bool ValidateState(const StateDisk& state, std::string* reason) {
  if (std::memcmp(state.magic, kMagic, sizeof(state.magic)) != 0) {
    *reason = "magic mismatch";
    return false;
  }
  if (state.ver != kStateVersion) {
    *reason = "version mismatch";
    return false;
  }
  if (state.size != sizeof(StateDisk)) {
    *reason = "size mismatch";
    return false;
  }
  if (!ValidateCommonFields(state.upgrade_available, state.mender_boot_part,
                            state.boot_action, reason)) {
    return false;
  }
  if (CalculateStateCrc(state) != state.crc32) {
    *reason = "crc mismatch";
    return false;
  }
  return true;
}

bool AtomicWriteState(const StateDisk& state) {
  if (!EnsureDirRecursive(kUpgradeDir)) {
    LOGE("state dir create failed: %s", kUpgradeDir);
    return false;
  }

  LOGI("atomic write state start path=%s tmp=%s", kStatePath, kStateTmpPath);

  const int fd =
      ::open(kStateTmpPath, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    LOGE("open tmp state failed: %s", std::strerror(errno));
    return false;
  }

  bool ok = WriteAll(fd, &state, sizeof(state));
  if (!ok) {
    LOGE("write tmp state failed: %s", std::strerror(errno));
  } else if (::fdatasync(fd) != 0) {
    LOGE("fdatasync tmp state failed: %s", std::strerror(errno));
    ok = false;
  }
  if (::close(fd) != 0) {
    LOGW("close tmp state failed: %s", std::strerror(errno));
  }

  if (!ok) {
    (void)::unlink(kStateTmpPath);
    return false;
  }

  if (::rename(kStateTmpPath, kStatePath) != 0) {
    LOGE("rename tmp state failed: %s", std::strerror(errno));
    (void)::unlink(kStateTmpPath);
    return false;
  }
  if (!FsyncDirectory(kUpgradeDir)) return false;
  LOGI("atomic write state done path=%s", kStatePath);
  return true;
}

bool AtomicSwitchCurrent(const char* slot_rel_path) {
  if (!EnsureDirRecursive(kAppFsDir)) {
    LOGE("appfs dir create failed: %s", kAppFsDir);
    return false;
  }

  LOGI("atomic switch current start current=%s target=%s", kCurrentPath,
       slot_rel_path);

  (void)::unlink(kCurrentTmpPath);
  if (::symlink(slot_rel_path, kCurrentTmpPath) != 0) {
    LOGE("create current tmp symlink failed: %s target=%s",
         std::strerror(errno), slot_rel_path);
    return false;
  }
  if (::rename(kCurrentTmpPath, kCurrentPath) != 0) {
    LOGE("rename current symlink failed: %s", std::strerror(errno));
    (void)::unlink(kCurrentTmpPath);
    return false;
  }
  if (!FsyncDirectory(kAppFsDir)) return false;
  LOGI("atomic switch current done current=%s target=%s", kCurrentPath,
       slot_rel_path);
  return true;
}

enum class LoadStateResult {
  kOk,
  kMissing,
  kInvalid,
  kIoError,
};

LoadStateResult LoadState(StateDisk* state, std::string* reason) {
  const int fd = ::open(kStatePath, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) return LoadStateResult::kMissing;
    *reason = std::string("open failed: ") + std::strerror(errno);
    return LoadStateResult::kIoError;
  }

  struct stat file_stat {};
  if (::fstat(fd, &file_stat) != 0) {
    *reason = std::string("fstat failed: ") + std::strerror(errno);
    (void)::close(fd);
    return LoadStateResult::kIoError;
  }
  if (file_stat.st_size != static_cast<off_t>(sizeof(StateDisk))) {
    *reason = "file size mismatch";
    (void)::close(fd);
    return LoadStateResult::kInvalid;
  }

  StateDisk raw_state {};
  if (!ReadAll(fd, &raw_state, sizeof(raw_state))) {
    *reason = std::string("read failed: ") + std::strerror(errno);
    (void)::close(fd);
    return LoadStateResult::kIoError;
  }
  if (::close(fd) != 0) {
    LOGW("close state file failed: %s", std::strerror(errno));
  }

  if (!ValidateState(raw_state, reason)) return LoadStateResult::kInvalid;
  *state = raw_state;
  return LoadStateResult::kOk;
}

void BackupBadState() {
  if (::access(kStatePath, F_OK) != 0) return;

  const std::time_t timestamp = std::time(nullptr);
  char backup_path[256];
  std::snprintf(backup_path, sizeof(backup_path), "%s.%lld.%d",
                kStateBadPrefix, static_cast<long long>(timestamp),
                static_cast<int>(::getpid()));
  if (::rename(kStatePath, backup_path) != 0) {
    LOGW("backup bad state failed: %s", std::strerror(errno));
    return;
  }
  LOGW("backup bad state to %s", backup_path);
}

bool PersistStateLocked() {
  g_state.crc32 = CalculateStateCrc(g_state);
  LogStateSnapshot("persist state", g_state);
  return AtomicWriteState(g_state);
}

bool LoadOrCreateState() {
  StateDisk loaded_state {};
  std::string reason;
  const LoadStateResult load_result = LoadState(&loaded_state, &reason);

  if (load_result == LoadStateResult::kOk) {
    bool need_rewrite = false;
    if (loaded_state.bootlimit == 0) {
      loaded_state.bootlimit = kDefaultBootLimit;
      need_rewrite = true;
      LOGW("state bootlimit=0, normalize to default=%u",
           static_cast<unsigned>(kDefaultBootLimit));
    }

    {
      std::lock_guard<std::mutex> lock(g_state_mu);
      g_state = loaded_state;
      LogStateSnapshot("state loaded", g_state);
      if (need_rewrite && !PersistStateLocked()) return false;
    }
    return true;
  }

  if (load_result == LoadStateResult::kInvalid) {
    LOGW("state invalid: %s", reason.c_str());
    BackupBadState();
  } else if (load_result == LoadStateResult::kMissing) {
    LOGI("state file missing, create default state");
  } else {
    LOGE("state load IO error: %s", reason.c_str());
  }

  std::lock_guard<std::mutex> lock(g_state_mu);
  g_state = MakeDefaultState();
  LogStateSnapshot("state create default", g_state);
  return PersistStateLocked();
}

const char* SlotRelPathForPart(uint8_t part) {
  return (part == kPartB) ? kSlotBRelPath : kSlotARelPath;
}

uint8_t OtherPart(uint8_t part) {
  return (part == kPartA) ? kPartB : kPartA;
}

void ClearLevel1History() {
  std::lock_guard<std::mutex> lock(g_level1_mu);
  g_level1_events.clear();
}

bool NoteLevel1RestartAndShouldReboot() {
  const int64_t now_ms = NowMsSteady();
  std::lock_guard<std::mutex> lock(g_level1_mu);
  g_level1_events.push_back(now_ms);
  while (!g_level1_events.empty() &&
         now_ms - g_level1_events.front() >
             static_cast<int64_t>(kWatchdogLevel1WindowMs)) {
    g_level1_events.pop_front();
  }
  LOGW("level1 failure recorded count=%zu limit=%u window_ms=%u", g_level1_events.size(),
       static_cast<unsigned>(kWatchdogLevel1Limit),
       static_cast<unsigned>(kWatchdogLevel1WindowMs));
  if (g_level1_events.size() < kWatchdogLevel1Limit) return false;
  g_level1_events.clear();
  return true;
}

bool MarkRollbackPendingLocked(uint32_t fail_reason) {
  g_state.boot_action = kBootActionRollback;
  g_state.last_fail_reason = fail_reason;
  LOGW("mark rollback pending reason=0x%08X", static_cast<unsigned>(fail_reason));
  return PersistStateLocked();
}

bool ExecuteRollbackLocked() {
  const uint8_t target_part = OtherPart(g_state.mender_boot_part);
  LOGW("execute rollback current_part=%s(%u) target_part=%s(%u)",
       PartName(g_state.mender_boot_part),
       static_cast<unsigned>(g_state.mender_boot_part), PartName(target_part),
       static_cast<unsigned>(target_part));
  LogStateSnapshot("rollback state before", g_state);
  g_state.mender_boot_part = target_part;
  g_state.upgrade_available = 0;
  g_state.bootcount = 0;
  g_state.bootlimit = (g_state.bootlimit == 0) ? kDefaultBootLimit
                                               : g_state.bootlimit;
  g_state.boot_action = kBootActionNormal;
  g_state.reboot_count = 0;
  if (!PersistStateLocked()) return false;
  return AtomicSwitchCurrent(SlotRelPathForPart(target_part));
}

bool PrepareStartup() {
  std::lock_guard<std::mutex> lock(g_state_mu);
  LogStateSnapshot("prepare startup", g_state);

  if (g_state.boot_action == kBootActionRollback) {
    LOGW("consume rollback action, current part=%u",
         static_cast<unsigned>(g_state.mender_boot_part));
    return ExecuteRollbackLocked();
  }

  if (g_state.upgrade_available == 1) {
    ++g_state.bootcount;
    if (!PersistStateLocked()) {
      LOGE("persist bootcount failed");
      return false;
    }

    if (g_state.bootcount > g_state.bootlimit) {
      LOGW("bootcount exceeded limit: bootcount=%u bootlimit=%u",
           static_cast<unsigned>(g_state.bootcount),
           static_cast<unsigned>(g_state.bootlimit));
      if (!MarkRollbackPendingLocked(kFailReasonBootcountExceeded)) {
        LOGE("mark rollback pending failed");
        return false;
      }
      return ExecuteRollbackLocked();
    }
  }

  return AtomicSwitchCurrent(SlotRelPathForPart(g_state.mender_boot_part));
}

bool ExecuteRollbackNow(uint32_t fail_reason) {
  std::lock_guard<std::mutex> lock(g_state_mu);
  LOGW("execute rollback now reason=0x%08X", static_cast<unsigned>(fail_reason));
  g_state.last_fail_reason = fail_reason;
  return ExecuteRollbackLocked();
}

void CloseFd(int* fd) {
  if (*fd >= 0) {
    (void)::close(*fd);
    *fd = -1;
  }
}

void CloseWatchdogClientLocked() { CloseFd(&g_watchdog.client_fd); }

bool SetNonBlocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) return false;
  return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool StartWatchdogServerLocked() {
  if (!EnsureDirRecursive(kUpgradeDir)) {
    LOGE("watchdog socket dir create failed: %s", kUpgradeDir);
    return false;
  }

  LOGI("watchdog socket server init path=%s", kWatchdogSockPath);
  (void)::unlink(kWatchdogSockPath);
  g_watchdog.server_fd = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
  if (g_watchdog.server_fd < 0) {
    LOGE("watchdog socket create failed: %s", std::strerror(errno));
    return false;
  }
  if (!SetNonBlocking(g_watchdog.server_fd)) {
    LOGE("set watchdog server nonblock failed: %s", std::strerror(errno));
    CloseFd(&g_watchdog.server_fd);
    return false;
  }

  sockaddr_un addr {};
  addr.sun_family = AF_UNIX;
  std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", kWatchdogSockPath);
  if (::bind(g_watchdog.server_fd, reinterpret_cast<sockaddr*>(&addr),
             sizeof(addr)) != 0) {
    LOGE("bind watchdog socket failed: %s", std::strerror(errno));
    CloseFd(&g_watchdog.server_fd);
    return false;
  }
  if (!FsyncDirectory(kUpgradeDir)) {
    CloseFd(&g_watchdog.server_fd);
    return false;
  }
  if (::listen(g_watchdog.server_fd, 1) != 0) {
    LOGE("listen watchdog socket failed: %s", std::strerror(errno));
    CloseFd(&g_watchdog.server_fd);
    return false;
  }
  LOGI("watchdog socket server ready path=%s", kWatchdogSockPath);
  return true;
}

void StopWatchdogServerLocked() {
  CloseWatchdogClientLocked();
  CloseFd(&g_watchdog.server_fd);
  (void)::unlink(kWatchdogSockPath);
}

void ResetWatchdogForSpawnLocked(pid_t child_pid) {
  g_watchdog.active_pid = child_pid;
  g_watchdog.child_start_ms = NowMsSteady();
  g_watchdog.last_valid_ping_ms = g_watchdog.child_start_ms;
  g_watchdog.healthy_since_ms = 0;
  g_watchdog.healthy_confirmed = false;
  g_watchdog.restart_requested = false;
  g_watchdog.restart_reason = 0;
  g_watchdog.last_bad_reason = 0;
  CloseWatchdogClientLocked();
}

void MarkWatchdogChildExitedLocked() {
  g_watchdog.active_pid = 0;
  g_watchdog.child_start_ms = 0;
  g_watchdog.last_valid_ping_ms = 0;
  g_watchdog.healthy_since_ms = 0;
  g_watchdog.healthy_confirmed = false;
  g_watchdog.last_bad_reason = 0;
  CloseWatchdogClientLocked();
}

bool ConsumeWatchdogRestart(uint32_t* reason) {
  std::lock_guard<std::mutex> lock(g_watchdog.mu);
  const bool requested = g_watchdog.restart_requested;
  *reason = g_watchdog.restart_reason;
  g_watchdog.restart_requested = false;
  g_watchdog.restart_reason = 0;
  return requested;
}

ChildExitHealth ConsumeChildExitHealth() {
  std::lock_guard<std::mutex> lock(g_watchdog.mu);
  ChildExitHealth health;
  health.healthy_confirmed = g_watchdog.healthy_confirmed;
  MarkWatchdogChildExitedLocked();
  return health;
}

bool OpenHardwareWatchdogLocked() {
  if (g_watchdog.hw_fd >= 0) return true;

  g_watchdog.hw_fd = ::open(kHardwareWatchdogPath, O_RDWR | O_CLOEXEC);
  if (g_watchdog.hw_fd < 0) {
    LOGW("open hardware watchdog failed: %s", std::strerror(errno));
    return false;
  }

  int timeout_sec = static_cast<int>(kWatchdogHardwareTimeoutSec);
  (void)::ioctl(g_watchdog.hw_fd, WDIOC_SETTIMEOUT, &timeout_sec);

  unsigned int option = WDIOS_ENABLECARD;
  (void)::ioctl(g_watchdog.hw_fd, WDIOC_SETOPTIONS, &option);
  LOGI("hardware watchdog enabled path=%s timeout_sec=%d", kHardwareWatchdogPath,
       timeout_sec);
  return true;
}

void FeedHardwareWatchdogLocked() {
  if (g_watchdog.hw_fd < 0 && !OpenHardwareWatchdogLocked()) return;
  (void)::ioctl(g_watchdog.hw_fd, WDIOC_KEEPALIVE, 0);
}

void CloseHardwareWatchdogLocked() { CloseFd(&g_watchdog.hw_fd); }

bool AcceptWatchdogClientLocked() {
  sockaddr_un addr {};
  socklen_t addr_len = sizeof(addr);
  const int client_fd = ::accept(g_watchdog.server_fd,
                                 reinterpret_cast<sockaddr*>(&addr),
                                 &addr_len);
  if (client_fd < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return true;
    LOGE("accept watchdog client failed: %s", std::strerror(errno));
    return false;
  }
  if (!SetNonBlocking(client_fd)) {
    LOGW("set watchdog client nonblock failed: %s", std::strerror(errno));
    (void)::close(client_fd);
    return true;
  }
  CloseWatchdogClientLocked();
  g_watchdog.client_fd = client_fd;
  LOGI("watchdog client accepted fd=%d", client_fd);
  return true;
}

bool HandleWatchdogPingLocked(const WatchdogPingV1& ping) {
  if (!IsValidWatchdogPingHeader(ping)) {
    LOGW("watchdog ping header invalid size=%u version=%u",
         static_cast<unsigned>(ping.size), static_cast<unsigned>(ping.version));
    return false;
  }
  if (g_watchdog.active_pid <= 0) {
    LOGW("watchdog ping ignored because no active child pid=%u",
         static_cast<unsigned>(ping.pid));
    return false;
  }
  if (ping.pid != static_cast<uint32_t>(g_watchdog.active_pid)) {
    LOGW("watchdog ping pid mismatch active_pid=%d ping_pid=%u",
         static_cast<int>(g_watchdog.active_pid),
         static_cast<unsigned>(ping.pid));
    return false;
  }
  if (g_watchdog.restart_requested) {
    LOGW("watchdog ping ignored because restart already requested reason=0x%08X",
         static_cast<unsigned>(g_watchdog.restart_reason));
    return false;
  }

  const bool threads_present =
      (ping.thread_bitmap & kWatchdogThreadMaskRequired) ==
      kWatchdogThreadMaskRequired;
  const bool threads_healthy =
      ping.net_rx_age_ms <= kWatchdogHeartbeatTimeoutMs &&
      ping.actor_age_ms <= kWatchdogHeartbeatTimeoutMs &&
      ping.net_tx_age_ms <= kWatchdogHeartbeatTimeoutMs &&
      ping.timer_age_ms <= kWatchdogHeartbeatTimeoutMs;

  if (!threads_present || !threads_healthy) {
    g_watchdog.last_bad_reason = kWatchdogFailThreadStall;
    g_watchdog.healthy_since_ms = 0;
    LOGW(
        "watchdog ping unhealthy threads_present=%u netrx=%u actor=%u nettx=%u "
        "timer=%u timeout_ms=%u",
        threads_present ? 1u : 0u, static_cast<unsigned>(ping.net_rx_age_ms),
        static_cast<unsigned>(ping.actor_age_ms),
        static_cast<unsigned>(ping.net_tx_age_ms),
        static_cast<unsigned>(ping.timer_age_ms),
        static_cast<unsigned>(kWatchdogHeartbeatTimeoutMs));
    return false;
  }

  const int64_t now_ms = NowMsSteady();
  g_watchdog.last_valid_ping_ms = now_ms;
  if (g_watchdog.healthy_since_ms == 0) {
    g_watchdog.healthy_since_ms = now_ms;
  }
  g_watchdog.last_bad_reason = 0;
  return true;
}

bool ReceiveWatchdogPingsLocked() {
  while (g_watchdog.client_fd >= 0) {
    WatchdogPingV1 ping {};
    const ssize_t rc =
        ::recv(g_watchdog.client_fd, &ping, sizeof(ping), MSG_DONTWAIT);
    if (rc == static_cast<ssize_t>(sizeof(ping))) {
      (void)HandleWatchdogPingLocked(ping);
      continue;
    }
    if (rc == 0) {
      CloseWatchdogClientLocked();
      return true;
    }
    if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
      return true;
    }
    LOGW("recv watchdog ping failed: %s", std::strerror(errno));
    CloseWatchdogClientLocked();
    return true;
  }
  return true;
}

void EvaluateWatchdogLocked(bool* should_reset_recovery) {
  *should_reset_recovery = false;

  const int64_t now_ms = NowMsSteady();
  const bool within_startup_grace =
      g_watchdog.active_pid > 0 &&
      now_ms - g_watchdog.child_start_ms <=
          static_cast<int64_t>(kWatchdogStartupGraceMs);
  const bool heartbeat_fresh =
      g_watchdog.active_pid > 0 && g_watchdog.last_valid_ping_ms > 0 &&
      now_ms - g_watchdog.last_valid_ping_ms <=
          static_cast<int64_t>(kWatchdogHeartbeatTimeoutMs);

  if (!g_watchdog.healthy_confirmed && g_watchdog.healthy_since_ms > 0 &&
      now_ms - g_watchdog.healthy_since_ms >=
          static_cast<int64_t>(kWatchdogHealthyConfirmMs)) {
    g_watchdog.healthy_confirmed = true;
    *should_reset_recovery = true;
    LOGI("watchdog healthy confirmed pid=%d healthy_ms=%lld",
         static_cast<int>(g_watchdog.active_pid),
         static_cast<long long>(now_ms - g_watchdog.healthy_since_ms));
  }

  if (g_watchdog.active_pid > 0 && !within_startup_grace && !heartbeat_fresh &&
      !g_watchdog.restart_requested) {
    g_watchdog.restart_requested = true;
    g_watchdog.restart_reason =
        (g_watchdog.last_bad_reason != 0) ? g_watchdog.last_bad_reason
                                          : kWatchdogFailHeartbeatTimeout;
    LOGW(
        "watchdog restart requested pid=%d reason=0x%08X within_startup_grace=%u "
        "last_ping_age_ms=%lld",
        static_cast<int>(g_watchdog.active_pid),
        static_cast<unsigned>(g_watchdog.restart_reason),
        within_startup_grace ? 1u : 0u,
        static_cast<long long>(now_ms - g_watchdog.last_valid_ping_ms));
    (void)::kill(g_watchdog.active_pid, SIGTERM);
  }

  if (g_stop_requested == 0 &&
      (g_watchdog.active_pid <= 0 || within_startup_grace || heartbeat_fresh)) {
    FeedHardwareWatchdogLocked();
  }
}

void WatchdogThreadMain() {
  {
    std::lock_guard<std::mutex> lock(g_watchdog.mu);
    if (!StartWatchdogServerLocked()) {
      g_watchdog_thread_failed.store(true);
      return;
    }
    g_watchdog_thread_ready.store(true);
    (void)OpenHardwareWatchdogLocked();
  }

  while (g_stop_requested == 0) {
    pollfd poll_fds[2];
    nfds_t nfds = 0;
    {
      std::lock_guard<std::mutex> lock(g_watchdog.mu);
      if (g_watchdog.server_fd >= 0) {
        poll_fds[nfds].fd = g_watchdog.server_fd;
        poll_fds[nfds].events = POLLIN;
        poll_fds[nfds].revents = 0;
        ++nfds;
      }
      if (g_watchdog.client_fd >= 0) {
        poll_fds[nfds].fd = g_watchdog.client_fd;
        poll_fds[nfds].events = POLLIN | POLLHUP | POLLERR;
        poll_fds[nfds].revents = 0;
        ++nfds;
      }
    }

    const int rc = ::poll(poll_fds, nfds, static_cast<int>(kPollMs));
    if (rc < 0 && errno != EINTR) {
      LOGE("poll watchdog socket failed: %s", std::strerror(errno));
    }

    bool should_reset_recovery = false;
    {
      std::lock_guard<std::mutex> lock(g_watchdog.mu);
      if (rc > 0) {
        for (nfds_t i = 0; i < nfds; ++i) {
          if ((poll_fds[i].revents & POLLIN) != 0 &&
              poll_fds[i].fd == g_watchdog.server_fd) {
            (void)AcceptWatchdogClientLocked();
          } else if ((poll_fds[i].revents & (POLLIN | POLLHUP | POLLERR)) != 0 &&
                     poll_fds[i].fd == g_watchdog.client_fd) {
            (void)ReceiveWatchdogPingsLocked();
          }
        }
      }
      EvaluateWatchdogLocked(&should_reset_recovery);
    }

    if (should_reset_recovery) {
      LOGI("software watchdog healthy confirmed, clear level1 failure window");
      ClearLevel1History();
    }
  }

  std::lock_guard<std::mutex> lock(g_watchdog.mu);
  StopWatchdogServerLocked();
  CloseHardwareWatchdogLocked();
}

bool SleepMsInterruptible(uint32_t ms) {
  uint32_t remaining_ms = ms;
  while (remaining_ms > 0) {
    if (g_stop_requested != 0) return false;
    const uint32_t step_ms = std::min<uint32_t>(remaining_ms, 100);
    struct timespec req {};
    req.tv_sec = static_cast<time_t>(step_ms / 1000);
    req.tv_nsec = static_cast<long>((step_ms % 1000) * 1000000L);
    while (::nanosleep(&req, &req) != 0) {
      if (errno != EINTR) break;
      if (g_stop_requested != 0) return false;
    }
    remaining_ms -= step_ms;
  }
  return g_stop_requested == 0;
}

pid_t SpawnChild() {
  const pid_t pid = ::fork();
  if (pid < 0) {
    LOGE("fork failed: %s", std::strerror(errno));
    return -1;
  }
  if (pid == 0) {
    if (::chdir(kAppFsDir) != 0) _exit(127);
    ::execl(kCurrentAppPath, kCurrentAppPath, static_cast<char*>(nullptr));
    _exit(127);
  }
  return pid;
}

const char* DescribeExit(int status, char* buffer, size_t buffer_len) {
  if (WIFEXITED(status)) {
    std::snprintf(buffer, buffer_len, "exited code=%d", WEXITSTATUS(status));
    return buffer;
  }
  if (WIFSIGNALED(status)) {
    std::snprintf(buffer, buffer_len, "killed by signal=%d", WTERMSIG(status));
    return buffer;
  }
  if (WIFSTOPPED(status)) {
    std::snprintf(buffer, buffer_len, "stopped by signal=%d", WSTOPSIG(status));
    return buffer;
  }
  std::snprintf(buffer, buffer_len, "status=0x%x", status);
  return buffer;
}

int EncodeExitReason(int status) {
  if (WIFEXITED(status)) return 0x1000 + WEXITSTATUS(status);
  if (WIFSIGNALED(status)) return 0x2000 + WTERMSIG(status);
  return 0;
}

/**
 * @brief 判断一次子进程退出是否应计入 Level 1 故障窗口。
 * @param child_health 子进程退出前的健康快照。
 * @param watchdog_requested 本次退出是否由守护进程软件狗主动触发。
 * @param exit_reason 子进程退出编码。
 * @param level1_reason 返回本次计入 Level 1 的原因码。
 * @return 需要把本次退出计入 Level 1 时返回 `true`。
 * @note WHY：软件狗超时只能覆盖“进程还活着但没有喂狗”的路径，覆盖不了空白固件、
 * 坏 ELF、`exec` 失败、启动后立刻退出这类“根本没进入健康态”的故障。若不把这类闪退
 * 纳入同一恢复链，现场会出现守护进程无限重拉错误程序、却永远不会 reboot/rollback 的
 * 死循环。
 */
bool ShouldCountExitAsLevel1(const ChildExitHealth& child_health,
                             bool watchdog_requested, uint32_t watchdog_reason,
                             int exit_reason, uint32_t* level1_reason) {
  if (watchdog_requested) {
    *level1_reason =
        (watchdog_reason != 0) ? watchdog_reason : static_cast<uint32_t>(exit_reason);
    return true;
  }
  if (child_health.healthy_confirmed || exit_reason == 0) return false;
  *level1_reason = static_cast<uint32_t>(exit_reason);
  return true;
}

int WaitChildWithShutdown(pid_t pid, bool* forced_kill) {
  *forced_kill = false;
  bool sent_term = false;
  auto deadline = std::chrono::steady_clock::time_point::min();
  int status = 0;

  for (;;) {
    const pid_t wait_result = ::waitpid(pid, &status, WNOHANG);
    if (wait_result == pid) return status;
    if (wait_result < 0) {
      if (errno == EINTR) continue;
      if (errno == ECHILD) return status;
      LOGE("waitpid failed: %s", std::strerror(errno));
      return status;
    }

    if (g_stop_requested != 0 && !sent_term) {
      if (::kill(pid, SIGTERM) != 0 && errno != ESRCH) {
        LOGW("send SIGTERM to child failed: %s", std::strerror(errno));
      }
      sent_term = true;
      deadline = std::chrono::steady_clock::now() +
                 std::chrono::milliseconds(kShutdownGraceMs);
    }

    if (sent_term && std::chrono::steady_clock::now() >= deadline) {
      if (::kill(pid, SIGKILL) != 0 && errno != ESRCH) {
        LOGW("send SIGKILL to child failed: %s", std::strerror(errno));
      } else {
        *forced_kill = true;
      }
      for (;;) {
        const pid_t final_wait = ::waitpid(pid, &status, 0);
        if (final_wait == pid) return status;
        if (final_wait < 0 && errno == EINTR) continue;
        if (final_wait < 0 && errno == ECHILD) return status;
        if (final_wait < 0) {
          LOGE("waitpid after SIGKILL failed: %s", std::strerror(errno));
        }
        return status;
      }
    }

    (void)SleepMsInterruptible(kPollMs);
  }
}

int AcquireLockOrExit(int* lock_fd) {
  if (!EnsureDirRecursive(kUpgradeDir)) {
    LOGE("lock dir create failed: %s", kUpgradeDir);
    return 2;
  }
  *lock_fd = ::open(kLockPath, O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (*lock_fd < 0) {
    LOGE("open lock file failed: %s", std::strerror(errno));
    return 2;
  }
  if (::flock(*lock_fd, LOCK_EX | LOCK_NB) != 0) {
    if (errno == EWOULDBLOCK || errno == EAGAIN) {
      LOGI("another upgraded instance is already running");
      return 0;
    }
    LOGE("flock lock file failed: %s", std::strerror(errno));
    return 2;
  }
  return 1;
}

void RunSupervisorLoop() {
  LOGI("supervisor loop start");
  g_watchdog_thread_ready.store(false);
  g_watchdog_thread_failed.store(false);
  std::thread watchdog_thread(WatchdogThreadMain);

  for (int i = 0; i < 20; ++i) {
    if (g_watchdog_thread_ready.load()) break;
    if (g_watchdog_thread_failed.load()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  if (!g_watchdog_thread_ready.load()) {
    g_stop_requested = 1;
    if (watchdog_thread.joinable()) watchdog_thread.join();
    LOGE("watchdog thread init failed");
    return;
  }

  uint32_t backoff_ms = kRestartBackoffInitMs;

  while (g_stop_requested == 0) {
    const pid_t child = SpawnChild();
    if (child < 0) {
      LOGW("spawn child failed, sleep backoff=%u ms", backoff_ms);
      (void)SleepMsInterruptible(backoff_ms);
      backoff_ms = std::min<uint32_t>(backoff_ms * 2, kRestartBackoffMaxMs);
      continue;
    }

    {
      std::lock_guard<std::mutex> lock(g_watchdog.mu);
      ResetWatchdogForSpawnLocked(child);
    }

    g_child_pid = static_cast<sig_atomic_t>(child);
    LOGI("child started pid=%d path=%s", static_cast<int>(child),
         kCurrentAppPath);

    bool forced_kill = false;
    const int status = WaitChildWithShutdown(child, &forced_kill);
    g_child_pid = -1;

    const ChildExitHealth child_health = ConsumeChildExitHealth();
    uint32_t watchdog_reason = 0;
    const bool watchdog_requested = ConsumeWatchdogRestart(&watchdog_reason);

    if (forced_kill) {
      LOGW("child killed after shutdown timeout (%u ms)", kShutdownGraceMs);
    }

    char reason_buf[64];
    LOGW("child pid=%d %s", static_cast<int>(child),
         DescribeExit(status, reason_buf, sizeof(reason_buf)));

    if (g_stop_requested != 0) break;

    const int exit_reason = EncodeExitReason(status);
    if (!watchdog_requested) {
      if (exit_reason != 0) {
        std::lock_guard<std::mutex> lock(g_state_mu);
        g_state.last_fail_reason = static_cast<uint32_t>(exit_reason);
        (void)PersistStateLocked();
      }
    }

    uint32_t level1_reason = watchdog_reason;
    if (ShouldCountExitAsLevel1(child_health, watchdog_requested, watchdog_reason,
                                exit_reason, &level1_reason)) {
      if (watchdog_requested) {
        LOGW("watchdog requested level1 restart reason=0x%04X",
             static_cast<unsigned>(level1_reason));
      } else {
        LOGW("child exited before healthy confirm, count as level1 reason=0x%04X",
             static_cast<unsigned>(level1_reason));
      }
      if (NoteLevel1RestartAndShouldReboot()) {
        LOGW("level1 threshold exceeded, rollback slot reason=0x%04X",
             static_cast<unsigned>(level1_reason));
        if (!ExecuteRollbackNow(level1_reason)) {
          LOGE("rollback flow failed");
          break;
        }
        ClearLevel1History();
        backoff_ms = kRestartBackoffInitMs;
        continue;
      }
    }

    LOGI("child restart backoff sleep=%u ms", backoff_ms);
    (void)SleepMsInterruptible(backoff_ms);
    backoff_ms = std::min<uint32_t>(backoff_ms * 2, kRestartBackoffMaxMs);
  }

  g_stop_requested = 1;
  if (watchdog_thread.joinable()) watchdog_thread.join();
}

}  // namespace

int RunUpgradedDaemon() {
  (void)btt::log::init_file_sink("/appfs/log", "upgraded.log", 512 * 1024, 4);
  LOGI("upgraded daemon start");

  if (!InstallSignalHandlers()) {
    LOGE("install signal handlers failed: %s", std::strerror(errno));
    return 2;
  }

  int lock_fd = -1;
  const int lock_result = AcquireLockOrExit(&lock_fd);
  if (lock_result == 0) return 0;
  if (lock_result != 1) return 2;
  LOGI("upgraded lock acquired path=%s", kLockPath);

  if (!LoadOrCreateState()) {
    LOGE("state init failed");
    if (lock_fd >= 0) (void)::close(lock_fd);
    return 2;
  }

  if (!PrepareStartup()) {
    LOGE("startup prepare failed");
    if (lock_fd >= 0) (void)::close(lock_fd);
    return 2;
  }
  LOGI("startup prepare done, current=%s app=%s", kCurrentPath, kCurrentAppPath);

  RunSupervisorLoop();

  if (lock_fd >= 0 && ::close(lock_fd) != 0) {
    LOGW("close lock fd failed: %s", std::strerror(errno));
  }
  LOGI("upgraded exit");
  return 0;
}

}  // namespace upgraded
