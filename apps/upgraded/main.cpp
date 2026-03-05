#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <string>

#include "libs/utils/log.hpp"

namespace {

#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__)
#error "upgraded requires little-endian target for StateV1Disk direct layout"
#endif

constexpr const char* kStateDir = "/appfs/upgrade";
constexpr const char* kStatePath = "/appfs/upgrade/state.bin";
constexpr const char* kStateTmpPath = "/appfs/upgrade/state.bin.tmp";
constexpr const char* kStateBadPrefix = "/appfs/upgrade/state.bin.bad";
constexpr const char* kLockPath = "/run/upgraded.lock";
constexpr const char* kAppPath = "/appfs/slots/A/app.bin";
constexpr const char* kAppWorkDir = "/appfs";

constexpr uint8_t kMagic[4] = {'U', 'P', 'G', '1'};
constexpr uint16_t kStateVersion = 1;
constexpr uint32_t kDefaultBootLimit = 3;
constexpr uint8_t kPartA = 2;
constexpr uint8_t kPartB = 3;
constexpr uint8_t kBootActionNormal = 1;
constexpr uint8_t kBootActionRollback = 2;

constexpr uint32_t kRestartBackoffInitMs = 200;
constexpr uint32_t kRestartBackoffMaxMs = 2000;
constexpr uint32_t kShutdownGraceMs = 2000;
constexpr uint32_t kPollMs = 100;

volatile sig_atomic_t g_stop_requested = 0;
volatile sig_atomic_t g_child_pid = -1;

#pragma pack(push, 1)
struct StateV1Disk {
  uint8_t magic[4];
  uint16_t ver;
  uint16_t size;
  uint8_t upgrade_available;
  uint32_t bootcount;
  uint32_t bootlimit;
  uint8_t mender_boot_part;
  uint8_t boot_action;
  uint32_t last_fail_reason;
  uint32_t reserved[4];
  uint32_t crc32;
};
#pragma pack(pop)

static_assert(sizeof(StateV1Disk) == 43, "StateV1Disk size must be stable");

void on_signal(int) {
  g_stop_requested = 1;
  const pid_t pid = static_cast<pid_t>(g_child_pid);
  if (pid > 0) {
    (void)::kill(pid, SIGTERM);
  }
}

bool install_signal_handlers() {
  struct sigaction sa {};
  sa.sa_handler = on_signal;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  if (::sigaction(SIGTERM, &sa, nullptr) != 0) {
    return false;
  }
  if (::sigaction(SIGINT, &sa, nullptr) != 0) {
    return false;
  }
  return true;
}

bool ensure_dir_recursive(const std::string& dir) {
  if (dir.empty()) return false;

  std::string cur;
  cur.reserve(dir.size());

  for (size_t i = 0; i < dir.size(); ++i) {
    cur.push_back(dir[i]);
    if (dir[i] != '/' && i + 1 != dir.size()) continue;
    if (cur == "/") continue;
    if (::mkdir(cur.c_str(), 0755) != 0 && errno != EEXIST) {
      return false;
    }
  }
  return true;
}

bool write_all(int fd, const void* data, size_t size) {
  const uint8_t* p = static_cast<const uint8_t*>(data);
  size_t written = 0;
  while (written < size) {
    const ssize_t rc = ::write(fd, p + written, size - written);
    if (rc < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (rc == 0) return false;
    written += static_cast<size_t>(rc);
  }
  return true;
}

bool read_all(int fd, void* data, size_t size) {
  uint8_t* p = static_cast<uint8_t*>(data);
  size_t consumed = 0;
  while (consumed < size) {
    const ssize_t rc = ::read(fd, p + consumed, size - consumed);
    if (rc < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (rc == 0) return false;
    consumed += static_cast<size_t>(rc);
  }
  return true;
}

uint32_t crc32_ieee(const uint8_t* data, size_t len) {
  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < len; ++i) {
    crc ^= static_cast<uint32_t>(data[i]);
    for (int bit = 0; bit < 8; ++bit) {
      if (crc & 1u) {
        crc = (crc >> 1u) ^ 0xEDB88320u;
      } else {
        crc >>= 1u;
      }
    }
  }
  return ~crc;
}

uint32_t calc_state_crc(const StateV1Disk& st) {
  return crc32_ieee(reinterpret_cast<const uint8_t*>(&st),
                    sizeof(StateV1Disk) - sizeof(st.crc32));
}

StateV1Disk make_default_state() {
  StateV1Disk st {};
  std::memcpy(st.magic, kMagic, sizeof(st.magic));
  st.ver = kStateVersion;
  st.size = static_cast<uint16_t>(sizeof(StateV1Disk));
  st.upgrade_available = 0;
  st.bootcount = 0;
  st.bootlimit = kDefaultBootLimit;
  st.mender_boot_part = kPartA;
  st.boot_action = kBootActionNormal;
  st.last_fail_reason = 0;
  std::memset(st.reserved, 0, sizeof(st.reserved));
  st.crc32 = calc_state_crc(st);
  return st;
}

bool validate_state(const StateV1Disk& st, std::string& why) {
  if (std::memcmp(st.magic, kMagic, sizeof(st.magic)) != 0) {
    why = "magic mismatch";
    return false;
  }
  if (st.ver != kStateVersion) {
    why = "version mismatch";
    return false;
  }
  if (st.size != sizeof(StateV1Disk)) {
    why = "size mismatch";
    return false;
  }
  if (st.upgrade_available > 1) {
    why = "upgrade_available invalid";
    return false;
  }
  if (st.mender_boot_part != kPartA && st.mender_boot_part != kPartB) {
    why = "mender_boot_part invalid";
    return false;
  }
  if (st.boot_action != kBootActionNormal && st.boot_action != kBootActionRollback) {
    why = "boot_action invalid";
    return false;
  }
  const uint32_t expect = calc_state_crc(st);
  if (expect != st.crc32) {
    why = "crc mismatch";
    return false;
  }
  return true;
}

bool atomic_write_state(const StateV1Disk& st) {
  if (!ensure_dir_recursive(kStateDir)) {
    LOGE("state dir create failed: %s", kStateDir);
    return false;
  }

  int fd = ::open(kStateTmpPath, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    LOGE("open tmp state failed: %s", std::strerror(errno));
    return false;
  }

  bool ok = write_all(fd, &st, sizeof(st));
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

  const int dir_fd = ::open(kStateDir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (dir_fd < 0) {
    LOGE("open state dir failed: %s", std::strerror(errno));
    return false;
  }
  if (::fsync(dir_fd) != 0) {
    LOGE("fsync state dir failed: %s", std::strerror(errno));
    (void)::close(dir_fd);
    return false;
  }
  if (::close(dir_fd) != 0) {
    LOGW("close state dir failed: %s", std::strerror(errno));
  }

  return true;
}

enum class LoadStateResult {
  kOk,
  kMissing,
  kInvalid,
  kIoError,
};

LoadStateResult load_state(StateV1Disk& st, std::string& why) {
  int fd = ::open(kStatePath, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) {
      return LoadStateResult::kMissing;
    }
    why = std::string("open failed: ") + std::strerror(errno);
    return LoadStateResult::kIoError;
  }

  struct stat fs {};
  if (::fstat(fd, &fs) != 0) {
    why = std::string("fstat failed: ") + std::strerror(errno);
    (void)::close(fd);
    return LoadStateResult::kIoError;
  }
  if (fs.st_size != static_cast<off_t>(sizeof(StateV1Disk))) {
    why = "file size mismatch";
    (void)::close(fd);
    return LoadStateResult::kInvalid;
  }

  if (!read_all(fd, &st, sizeof(st))) {
    why = std::string("read failed: ") + std::strerror(errno);
    (void)::close(fd);
    return LoadStateResult::kIoError;
  }
  if (::close(fd) != 0) {
    LOGW("close state file failed: %s", std::strerror(errno));
  }

  if (!validate_state(st, why)) {
    return LoadStateResult::kInvalid;
  }
  return LoadStateResult::kOk;
}

void backup_bad_state() {
  if (::access(kStatePath, F_OK) != 0) {
    return;
  }

  const std::time_t ts = std::time(nullptr);
  char backup_path[256];
  std::snprintf(backup_path, sizeof(backup_path), "%s.%lld.%d", kStateBadPrefix,
                static_cast<long long>(ts), static_cast<int>(::getpid()));

  if (::rename(kStatePath, backup_path) != 0) {
    LOGW("backup bad state failed: %s", std::strerror(errno));
    return;
  }
  LOGW("backup bad state to %s", backup_path);
}

bool ensure_state_ready() {
  StateV1Disk st {};
  std::string why;
  const LoadStateResult rc = load_state(st, why);

  if (rc == LoadStateResult::kOk) {
    if (st.bootlimit == 0) {
      LOGW("state bootlimit is 0, rewrite default bootlimit=%u", kDefaultBootLimit);
      st.bootlimit = kDefaultBootLimit;
      st.crc32 = calc_state_crc(st);
      return atomic_write_state(st);
    }
    return true;
  }

  if (rc == LoadStateResult::kInvalid) {
    LOGW("state invalid: %s", why.c_str());
    backup_bad_state();
  } else if (rc == LoadStateResult::kMissing) {
    LOGI("state file missing, create default state");
  } else {
    LOGE("state load IO error: %s", why.c_str());
  }

  const StateV1Disk def = make_default_state();
  return atomic_write_state(def);
}

bool sleep_ms_interruptible(uint32_t ms) {
  uint32_t remain_ms = ms;
  while (remain_ms > 0) {
    if (g_stop_requested) return false;
    const uint32_t step = std::min<uint32_t>(remain_ms, 100);
    struct timespec req {};
    req.tv_sec = static_cast<time_t>(step / 1000);
    req.tv_nsec = static_cast<long>((step % 1000) * 1000000L);
    while (::nanosleep(&req, &req) != 0) {
      if (errno != EINTR) break;
      if (g_stop_requested) return false;
    }
    remain_ms -= step;
  }
  return !g_stop_requested;
}

pid_t spawn_child() {
  const pid_t pid = ::fork();
  if (pid < 0) {
    LOGE("fork failed: %s", std::strerror(errno));
    return -1;
  }
  if (pid == 0) {
    if (::chdir(kAppWorkDir) != 0) {
      _exit(127);
    }
    ::execl(kAppPath, kAppPath, static_cast<char*>(nullptr));
    _exit(127);
  }
  return pid;
}

const char* describe_exit(int status, char* buf, size_t len) {
  if (WIFEXITED(status)) {
    std::snprintf(buf, len, "exited code=%d", WEXITSTATUS(status));
    return buf;
  }
  if (WIFSIGNALED(status)) {
    std::snprintf(buf, len, "killed by signal=%d", WTERMSIG(status));
    return buf;
  }
  if (WIFSTOPPED(status)) {
    std::snprintf(buf, len, "stopped by signal=%d", WSTOPSIG(status));
    return buf;
  }
  std::snprintf(buf, len, "status=0x%x", status);
  return buf;
}

int wait_child_with_shutdown(pid_t pid, bool& forced_kill) {
  forced_kill = false;
  bool sent_term = false;
  auto deadline = std::chrono::steady_clock::time_point::min();
  int status = 0;

  for (;;) {
    const pid_t rc = ::waitpid(pid, &status, WNOHANG);
    if (rc == pid) {
      return status;
    }
    if (rc < 0) {
      if (errno == EINTR) continue;
      if (errno == ECHILD) return status;
      LOGE("waitpid failed: %s", std::strerror(errno));
      return status;
    }

    if (g_stop_requested && !sent_term) {
      if (::kill(pid, SIGTERM) != 0 && errno != ESRCH) {
        LOGW("send SIGTERM to child failed: %s", std::strerror(errno));
      }
      sent_term = true;
      deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kShutdownGraceMs);
    }

    if (sent_term && std::chrono::steady_clock::now() >= deadline) {
      if (::kill(pid, SIGKILL) != 0 && errno != ESRCH) {
        LOGW("send SIGKILL to child failed: %s", std::strerror(errno));
      } else {
        forced_kill = true;
      }
      for (;;) {
        const pid_t wait_rc = ::waitpid(pid, &status, 0);
        if (wait_rc == pid) return status;
        if (wait_rc < 0 && errno == EINTR) continue;
        if (wait_rc < 0 && errno == ECHILD) return status;
        if (wait_rc < 0) LOGE("waitpid after SIGKILL failed: %s", std::strerror(errno));
        return status;
      }
    }

    (void)sleep_ms_interruptible(kPollMs);
  }
}

int acquire_lock_or_exit(int& lock_fd) {
  lock_fd = ::open(kLockPath, O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (lock_fd < 0) {
    LOGE("open lock file failed: %s", std::strerror(errno));
    return 2;
  }
  if (::flock(lock_fd, LOCK_EX | LOCK_NB) != 0) {
    if (errno == EWOULDBLOCK || errno == EAGAIN) {
      LOGI("another upgraded instance is already running");
      return 0;
    }
    LOGE("flock lock file failed: %s", std::strerror(errno));
    return 2;
  }
  return 1;
}

}  // namespace

int main() {
  (void)btt::log::init_file_sink("/appfs/log", "upgraded.log", 512 * 1024, 4);

  if (!install_signal_handlers()) {
    LOGE("install signal handlers failed: %s", std::strerror(errno));
    return 2;
  }

  int lock_fd = -1;
  const int lock_result = acquire_lock_or_exit(lock_fd);
  if (lock_result == 0) return 0;
  if (lock_result != 1) return 2;

  if (!ensure_state_ready()) {
    LOGE("state init failed, daemon keeps running for watchdog");
  }

  uint32_t backoff_ms = kRestartBackoffInitMs;
  while (!g_stop_requested) {
    const pid_t child = spawn_child();
    if (child < 0) {
      (void)sleep_ms_interruptible(backoff_ms);
      backoff_ms = std::min<uint32_t>(backoff_ms * 2, kRestartBackoffMaxMs);
      continue;
    }

    g_child_pid = static_cast<sig_atomic_t>(child);
    LOGI("child started pid=%d path=%s", static_cast<int>(child), kAppPath);

    bool forced_kill = false;
    int status = wait_child_with_shutdown(child, forced_kill);
    g_child_pid = -1;

    if (forced_kill) {
      LOGW("child killed after shutdown timeout (%u ms)", kShutdownGraceMs);
    }

    char reason[64];
    LOGW("child pid=%d %s", static_cast<int>(child), describe_exit(status, reason, sizeof(reason)));

    if (g_stop_requested) {
      break;
    }

    (void)sleep_ms_interruptible(backoff_ms);
    backoff_ms = std::min<uint32_t>(backoff_ms * 2, kRestartBackoffMaxMs);
  }

  if (lock_fd >= 0 && ::close(lock_fd) != 0) {
    LOGW("close lock fd failed: %s", std::strerror(errno));
  }

  LOGI("upgraded exit");
  return 0;
}
