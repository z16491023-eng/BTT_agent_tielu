/**
 * @file
 * @brief btt_core 核心实现。
 *
 * Invariants:
 * - actor 线程是协议状态机语义的唯一写入者；网络线程、定时线程和媒体线程只通过队列或原子量协作。
 * - 所有需要跨重启保留的核心元数据，都必须通过“临时文件落盘后原子替换”的路径提交，避免掉电产生半写状态。
 */

#include "libs/core/core_app.hpp"

#include <atomic>
#include <thread>
#include <string>
#include <chrono>
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <vector>
#include <array>
#include <mutex>
#include <condition_variable>
#include <ctime>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <dirent.h>
#include <algorithm>
#include <deque>
#include <cstddef>

//网络
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <linux/route.h>
#include <errno.h>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <sys/wait.h>






extern "C" {
#include "HiF_media_ss522.h"
#include "hgs_misc.h"
}

#include "libs/utils/log.hpp"
#include "libs/utils/blocking_queue.hpp"
#include "libs/proto/btt_proto.hpp"
#include "libs/net/tcp_client.hpp"
#include "upgraded/upgrade_control_protocol.h"
#include "upgraded/watchdog_protocol.h"

namespace btt::core {

// ---------- 基础 ----------
/**
 * @brief 获取单调时钟毫秒时间戳。
 * @return 返回计算结果。
 */
inline int64_t now_ms_steady() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

/**
 * @brief 计算两个单调时钟时间戳之间的毫秒差。
 * @param now_ms 时间长度，单位毫秒。
 * @param last_ms 时间长度，单位毫秒。
 * @return 返回计算结果。
 */
inline uint32_t age_ms_from(int64_t now_ms, int64_t last_ms) {
  if (last_ms <= 0 || now_ms <= last_ms) return 0;
  const int64_t delta = now_ms - last_ms;
  return static_cast<uint32_t>(
      (delta > static_cast<int64_t>(UINT32_MAX)) ? UINT32_MAX : delta);
}

/**
 * @brief 判断套接字错误码是否更接近“网络掉线/不可达”。
 * @param err 套接字错误码。
 * @return `true` 表示应归类为重连原因 `13`，否则返回 `false`。
 */
inline bool is_network_drop_errno(int err) {
  switch (err) {
    case ENETDOWN:
    case ENETUNREACH:
    case ENONET:
    case EHOSTDOWN:
    case EHOSTUNREACH:
      return true;
    default:
      return false;
  }
}

/**
 * @brief 将套接字错误码映射为认证重连原因。
 * @param err 套接字错误码。
 * @return 返回协议中的重连原因码。
 */
inline uint8_t reconnect_reason_from_socket_error(int err) {
  if (is_network_drop_errno(err)) return 13;
  if (err == ECONNRESET) return 15;
  if (err == EPIPE) return 14;
  return 16;
}

class UpgradedWatchdogClient {
 public:
  /**
   * @brief 析构 `UpgradedWatchdogClient` 对象并释放关联资源。
   */
  ~UpgradedWatchdogClient() { Close(); }

  /**
   * @brief 向升级看门狗发送心跳。
   * @param ping 待发送的看门狗心跳报文。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool SendPing(const upgraded::WatchdogPingV1& ping) {
    if (fd_ < 0 && !Connect()) return false;

    const ssize_t rc =
        ::send(fd_, &ping, sizeof(ping), MSG_DONTWAIT | MSG_NOSIGNAL);
    if (rc == static_cast<ssize_t>(sizeof(ping))) return true;

    if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return false;
    Close();
    return false;
  }

 private:
  /**
   * @brief 建立升级看门狗连接。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool Connect() {
    Close();

    fd_ = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (fd_ < 0) return false;

    const int flags = ::fcntl(fd_, F_GETFL, 0);
    if (flags >= 0) {
      (void)::fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
    }

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s",
                  upgraded::kWatchdogSockPath);

    const int rc = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr),
                             sizeof(addr));
    if (rc == 0 || errno == EISCONN) return true;

    Close();
    return false;
  }

  /**
   * @brief 关闭升级看门狗连接。
   */
  void Close() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  int fd_ = -1;
};

class UpgradedControlClient {
 public:
  /**
   * @brief 请求守护进程提交已接收完成的升级包。
   * @param pid 当前 `btt-core` 进程号。
   * @param pkg_size 应用侧写入 `pkg.part` 的总字节数。
   * @param response 成功收到合法应答时写回应答内容。
   * @return `true` 表示收到了合法应答；`false` 表示本地 IPC 失败。
   */
  bool CommitPackage(uint32_t pid, uint32_t pkg_size,
                     upgraded::UpgradeCtlResponseV1* response) {
    upgraded::UpgradeCtlRequestV1 request{};
    upgraded::InitUpgradeCtlRequest(&request,
                                    upgraded::kUpgradeCtlCommandCommitPackage);
    request.pid = pid;
    request.value0 = pkg_size;
    return RoundTrip(request, response);
  }

  /**
   * @brief 查询守护进程侧是否存在待上报的升级结果。
   * @param pid 当前 `btt-core` 进程号。
   * @param response 成功收到合法应答时写回应答内容。
   * @return `true` 表示收到了合法应答；`false` 表示本地 IPC 失败。
   */
  bool QueryReport(uint32_t pid, upgraded::UpgradeCtlResponseV1* response) {
    upgraded::UpgradeCtlRequestV1 request{};
    upgraded::InitUpgradeCtlRequest(&request,
                                    upgraded::kUpgradeCtlCommandQueryReport);
    request.pid = pid;
    return RoundTrip(request, response);
  }

  /**
   * @brief 在 `0x58` 获得上位机 ACK 后清除守护进程侧的待上报结果。
   * @param pid 当前 `btt-core` 进程号。
   * @param response 成功收到合法应答时写回应答内容。
   * @return `true` 表示收到了合法应答；`false` 表示本地 IPC 失败。
   */
  bool ClearReport(uint32_t pid, upgraded::UpgradeCtlResponseV1* response) {
    upgraded::UpgradeCtlRequestV1 request{};
    upgraded::InitUpgradeCtlRequest(&request,
                                    upgraded::kUpgradeCtlCommandClearReport);
    request.pid = pid;
    return RoundTrip(request, response);
  }

 private:
  /**
   * @brief 执行一次本地升级控制 RPC。
   * @param request 请求报文。
   * @param response 应答输出参数。
   * @return `true` 表示收到了合法应答。
   */
  bool RoundTrip(const upgraded::UpgradeCtlRequestV1& request,
                 upgraded::UpgradeCtlResponseV1* response) {
    const int fd = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (fd < 0) return false;

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s",
                  upgraded::kUpgradeControlSockPath);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      ::close(fd);
      return false;
    }

    const ssize_t send_rc =
        ::send(fd, &request, sizeof(request), MSG_NOSIGNAL);
    if (send_rc != static_cast<ssize_t>(sizeof(request))) {
      ::close(fd);
      return false;
    }

    const ssize_t recv_rc = ::recv(fd, response, sizeof(*response), 0);
    ::close(fd);
    return recv_rc == static_cast<ssize_t>(sizeof(*response)) &&
           upgraded::IsValidUpgradeCtlResponseHeader(*response);
  }
};

/**
 * @brief 升级包分包接收状态机。
 *
 * @note WHY：`0x3C` 采用停等 + 重发模型，下位机必须对“重包”和“乱序包”作出不同处理。
 * 若把重复包也重新写进 `pkg.part`，会直接破坏目标固件；若对同一包重复回失败 ACK，又会让
 * 上位机在链路抖动时无法收敛，因此这里需要显式记录“最近一次成功 ACK”的包序号并支持去重。
 */
class UpgradeIngress {
 public:
  ~UpgradeIngress() { ClosePartFile(); }

  /**
   * @brief 处理一帧 `0x3C` 升级包数据。
   * @param fr 协议帧。
   * @param ctl_client 守护进程控制客户端。
   * @return 返回针对该帧构造好的 `0x3C` ACK。
   */
  btt::proto::Frame HandlePacket(const btt::proto::Frame& fr,
                                 UpgradedControlClient& ctl_client) {
    uint8_t op = 0;
    uint8_t flag = 0;
    uint16_t pkt_seq = 0;
    const uint8_t* data = nullptr;
    size_t data_len = 0;
    if (!ParsePacket(fr, &op, &flag, &pkt_seq, &data, &data_len)) {
      return BuildAck(fr.seq, 0, 0, 0, upgraded::kUpgradeCtlResultFail,
                      upgraded::kUpgradeCtlReasonParamInvalid);
    }
    if (flag > 2) {
      return RememberAck(BuildAck(fr.seq, op, flag, pkt_seq,
                                  upgraded::kUpgradeCtlResultFail,
                                  upgraded::kUpgradeCtlReasonParamInvalid));
    }
    if (op != 0) {
      return RememberAck(BuildAck(fr.seq, op, flag, pkt_seq,
                                  upgraded::kUpgradeCtlResultFail,
                                  upgraded::kUpgradeCtlReasonParamInvalid));
    }

    if (last_ack_valid_ && pkt_seq == last_ack_seq_ && flag == last_ack_flag_) {
      LOGW("0x3C duplicate packet seq=%u flag=%u, resend last ack", pkt_seq, flag);
      return BuildAck(fr.seq, op, flag, pkt_seq, last_ack_result_, last_ack_reason_);
    }

    if (flag == 0) {
      return HandleStartPacket(fr.seq, op, flag, pkt_seq, data, data_len);
    }
    if (!active_) {
      Reset(false);
      return RememberAck(BuildAck(fr.seq, op, flag, pkt_seq,
                                  upgraded::kUpgradeCtlResultFail,
                                  upgraded::kUpgradeCtlReasonStateConflict));
    }
    if (pkt_seq != expected_seq_) {
      Reset(true);
      return RememberAck(BuildAck(fr.seq, op, flag, pkt_seq,
                                  upgraded::kUpgradeCtlResultFail,
                                  upgraded::kUpgradeCtlReasonSequenceInvalid));
    }

    if (!WriteChunk(data, data_len)) {
      Reset(true);
      return RememberAck(BuildAck(fr.seq, op, flag, pkt_seq,
                                  upgraded::kUpgradeCtlResultFail,
                                  upgraded::kUpgradeCtlReasonWritePkgPartFailed));
    }
    ++expected_seq_;

    if (flag == 2) {
      if (!FinishAndCommit(ctl_client)) {
        Reset(true);
        return RememberAck(BuildAck(fr.seq, op, flag, pkt_seq,
                                    upgraded::kUpgradeCtlResultFail,
                                    upgraded::kUpgradeCtlReasonCommitFailed));
      }
      ClosePartFile();
      active_ = false;
      quiet_mode_ = false;
      install_pending_ = true;
      expected_seq_ = 0;
      received_bytes_ = 0;
    }

    return RememberAck(BuildAck(fr.seq, op, flag, pkt_seq,
                                upgraded::kUpgradeCtlResultOk,
                                upgraded::kUpgradeCtlReasonNone));
  }

  /**
   * @brief 查询是否处于升级静默模式。
   * @return 静默模式开启时返回 `true`。
   */
  bool quiet_mode() const { return quiet_mode_; }

  /**
   * @brief 在链路中断时终止尚未提交完成的升级接收会话。
   */
  void AbortActiveTransfer() {
    if (!active_) return;
    LOGW("abort active upgrade transfer due to disconnect");
    Reset(true);
  }

 private:
  static constexpr const char* kUpgradeDir = "/appfs/upgrade";
  static constexpr const char* kPkgPartPath = "/appfs/upgrade/pkg.part";

  bool ParsePacket(const btt::proto::Frame& fr, uint8_t* op, uint8_t* flag,
                   uint16_t* pkt_seq, const uint8_t** data,
                   size_t* data_len) const {
    if (fr.payload.size() < 4) return false;
    *op = fr.payload[0];
    *flag = fr.payload[1];
    *pkt_seq = uint16_t((uint16_t(fr.payload[2]) << 8) | fr.payload[3]);
    *data = fr.payload.data() + 4;
    *data_len = fr.payload.size() - 4;
    return true;
  }

  static bool EnsureUpgradeDir() {
    if (::mkdir("/appfs", 0755) != 0 && errno != EEXIST) return false;
    if (::mkdir(kUpgradeDir, 0755) != 0 && errno != EEXIST) return false;
    return true;
  }

  static bool FsyncDir(const char* path) {
    const int dir_fd = ::open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dir_fd < 0) return false;
    const int rc = ::fsync(dir_fd);
    ::close(dir_fd);
    return rc == 0;
  }

  static bool WriteAllBytes(int fd, const void* data, size_t size) {
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    size_t written = 0;
    while (written < size) {
      const ssize_t rc = ::write(fd, bytes + written, size - written);
      if (rc < 0) {
        if (errno == EINTR) continue;
        return false;
      }
      if (rc == 0) return false;
      written += static_cast<size_t>(rc);
    }
    return true;
  }

  btt::proto::Frame BuildAck(uint16_t frame_seq, uint8_t op, uint8_t flag,
                             uint16_t pkt_seq, uint8_t result,
                             uint8_t reason) const {
    btt::proto::Frame ack;
    ack.cmd = 0x3C;
    ack.level = 0x01;
    ack.seq = frame_seq;
    ack.payload.reserve(6);
    ack.payload.push_back(result);
    ack.payload.push_back(reason);
    ack.payload.push_back(op);
    ack.payload.push_back(flag);
    ack.payload.push_back(uint8_t((pkt_seq >> 8) & 0xFF));
    ack.payload.push_back(uint8_t(pkt_seq & 0xFF));
    return ack;
  }

  btt::proto::Frame RememberAck(btt::proto::Frame ack) {
    if (ack.payload.size() >= 6) {
      last_ack_valid_ = true;
      last_ack_result_ = ack.payload[0];
      last_ack_reason_ = ack.payload[1];
      last_ack_flag_ = ack.payload[3];
      last_ack_seq_ =
          uint16_t((uint16_t(ack.payload[4]) << 8) | ack.payload[5]);
    }
    return ack;
  }

  btt::proto::Frame HandleStartPacket(uint16_t frame_seq, uint8_t op, uint8_t flag,
                                      uint16_t pkt_seq, const uint8_t* data,
                                      size_t data_len) {
    if (quiet_mode_ || active_ || install_pending_) {
      return RememberAck(BuildAck(frame_seq, op, flag, pkt_seq,
                                  upgraded::kUpgradeCtlResultFail,
                                  upgraded::kUpgradeCtlReasonBusy));
    }
    if (pkt_seq != 0) {
      return RememberAck(BuildAck(frame_seq, op, flag, pkt_seq,
                                  upgraded::kUpgradeCtlResultFail,
                                  upgraded::kUpgradeCtlReasonSequenceInvalid));
    }
    if (!EnsureUpgradeDir()) {
      return RememberAck(BuildAck(frame_seq, op, flag, pkt_seq,
                                  upgraded::kUpgradeCtlResultFail,
                                  upgraded::kUpgradeCtlReasonWritePkgPartFailed));
    }

    (void)::unlink(kPkgPartPath);
    part_fd_ =
        ::open(kPkgPartPath, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
    if (part_fd_ < 0) {
      return RememberAck(BuildAck(frame_seq, op, flag, pkt_seq,
                                  upgraded::kUpgradeCtlResultFail,
                                  upgraded::kUpgradeCtlReasonWritePkgPartFailed));
    }

    quiet_mode_ = true;
    active_ = true;
    expected_seq_ = 0;
    received_bytes_ = 0;

    if (!WriteChunk(data, data_len)) {
      Reset(true);
      return RememberAck(BuildAck(frame_seq, op, flag, pkt_seq,
                                  upgraded::kUpgradeCtlResultFail,
                                  upgraded::kUpgradeCtlReasonWritePkgPartFailed));
    }
    expected_seq_ = 1;
    LOGI("0x3C start accepted seq=%u data_len=%zu", pkt_seq, data_len);
    return RememberAck(BuildAck(frame_seq, op, flag, pkt_seq,
                                upgraded::kUpgradeCtlResultOk,
                                upgraded::kUpgradeCtlReasonNone));
  }

  bool WriteChunk(const uint8_t* data, size_t data_len) {
    if (part_fd_ < 0) return false;
    if (data_len == 0) return true;
    if (!WriteAllBytes(part_fd_, data, data_len)) return false;
    received_bytes_ += static_cast<uint32_t>(data_len);
    return true;
  }

  bool FinishAndCommit(UpgradedControlClient& ctl_client) {
    if (part_fd_ < 0) return false;
    if (::fdatasync(part_fd_) != 0) {
      LOGW("fdatasync pkg.part failed: %s", std::strerror(errno));
      return false;
    }
    if (::close(part_fd_) != 0) {
      LOGW("close pkg.part failed: %s", std::strerror(errno));
      part_fd_ = -1;
      return false;
    }
    part_fd_ = -1;
    if (!FsyncDir(kUpgradeDir)) {
      LOGW("fsync upgrade dir failed after pkg.part");
      return false;
    }

    upgraded::UpgradeCtlResponseV1 response{};
    if (!ctl_client.CommitPackage(static_cast<uint32_t>(::getpid()),
                                  received_bytes_, &response)) {
      LOGW("commit package rpc failed");
      return false;
    }
    if (response.result != upgraded::kUpgradeCtlResultOk) {
      LOGW("commit package rejected reason=0x%02X phase=%u target=%u", response.reason,
           static_cast<unsigned>(response.upgrade_phase),
           static_cast<unsigned>(response.target_part));
      return false;
    }
    LOGI("0x3C end committed size=%u target=%u", received_bytes_,
         static_cast<unsigned>(response.target_part));
    return true;
  }

  void ClosePartFile() {
    if (part_fd_ >= 0) {
      ::close(part_fd_);
      part_fd_ = -1;
    }
  }

  void Reset(bool drop_part_file) {
    ClosePartFile();
    if (drop_part_file) (void)::unlink(kPkgPartPath);
    active_ = false;
    quiet_mode_ = false;
    install_pending_ = false;
    expected_seq_ = 0;
    received_bytes_ = 0;
  }

  bool active_ = false;
  bool quiet_mode_ = false;
  bool install_pending_ = false;
  int part_fd_ = -1;
  uint16_t expected_seq_ = 0;
  uint32_t received_bytes_ = 0;
  bool last_ack_valid_ = false;
  uint16_t last_ack_seq_ = 0;
  uint8_t last_ack_flag_ = 0;
  uint8_t last_ack_result_ = upgraded::kUpgradeCtlResultOk;
  uint8_t last_ack_reason_ = upgraded::kUpgradeCtlReasonNone;
};

// 新增 ID 规则：
// - 事件 ID: 高 4bit = 0x1
// - 文件 ID: 高 4bit = 0x2
// 旧数据不满足该规则时，仍通过目录/文件实际存在性兼容处理。
static constexpr uint32_t kIdBodyMask = 0x0FFFFFFFu;
static constexpr uint32_t kEventIdTag = 0x10000000u;
static constexpr uint32_t kFileIdTag = 0x20000000u;

/**
 * @brief 构造 `make_tagged_id` 对应结果。
 * @param raw 原始数值。
 * @param tag 标签值。
 * @return 返回计算结果。
 */
inline uint32_t make_tagged_id(uint32_t raw, uint32_t tag) {
  return tag | (raw & kIdBodyMask);
}

/**
 * @brief 分配 `alloc_tagged_id` 对应标识。
 * @param seq 协议序号。
 * @param tag 标签值。
 * @return 返回计算结果。
 */
inline uint32_t alloc_tagged_id(std::atomic<uint32_t>& seq, uint32_t tag) {
  const uint32_t raw = seq.fetch_add(1);
  return make_tagged_id(raw, tag);
}

enum class CoreControlAction : uint8_t {
  None = 0,
  RebootDevice = 1,
  RestartProgram = 2,
};

static constexpr const char* kFactoryServerIpStr = "192.168.14.250";
static constexpr uint32_t kFactoryServerIpBe = 0xC0A80EFAu;   // 192.168.14.250
static constexpr uint32_t kFactoryDevIpBe = 0xC0A80E26u;      // 192.168.14.38
static constexpr uint32_t kFactoryGatewayIpBe = 0xC0A80E01u;  // 192.168.14.1
static constexpr uint32_t kFactoryNetmaskBe = 0xFFFFFF00u;    // 255.255.255.0
static constexpr uint8_t kFactoryMediaPicRes = 3;
static constexpr uint8_t kFactoryMediaPicQuality = 75;
static constexpr uint8_t kFactoryMediaVideoRes = 3;
static constexpr uint8_t kFactoryMediaVideoFps = 25;
static constexpr uint8_t kFactoryMediaVideoBitrateUnit = 17;
static constexpr uint8_t kFactoryHbIntervalS = 5;
static constexpr uint8_t kFactoryBusinessTimeoutS = 0;
static constexpr uint8_t kFactoryReconnectIntervalS = 5;
static constexpr uint8_t kFactoryNoCommRebootS = 0;
static constexpr uint8_t kFactoryTriggerMode = 1;
static constexpr uint8_t kFactoryMeasureIntervalMin = 30;


struct EventMetaV1 {
  uint8_t ver = 1;
  uint8_t rsv0 = 0;      // reserved (was "reported" flag)
  uint8_t event_type = 2; // 默认：上位机事件
  uint8_t cur_pos = 3;   // 默认：不确定
  uint32_t event_id = 0;
  uint8_t start_bcd[8]{};
  uint8_t end_bcd[8]{};
  uint16_t hydraulic = 0;
  uint16_t level = 0;
  uint32_t temp_humi = 0;
  uint32_t gap_value = 0;
};

struct FileInfo25 {
  uint32_t file_id = 0;//文件唯一标识ID。
  uint32_t file_len = 0;//文件长度	
  uint8_t  start_bcd[8]{};//开始时间	定义初始化0数组
  uint8_t  flag = 0xFF;//标志
  uint32_t duration_ms = 0;//持续时间（毫秒）
  uint8_t  pos_mask = 0;   // 摄像头ID（0..3）
  uint16_t reserved = 0;//保留
  uint8_t  file_type = 0;  // Bit7激光 Bit6补光 Bit4-5 camId 低4位文件类型(0图片/1录像/5配置)
};

inline bool write_meta_v1(const std::string& dir, const EventMetaV1& m, const std::vector<FileInfo25>& files);
inline bool read_meta_v1(const std::string& dir, EventMetaV1& m, std::vector<FileInfo25>& files);
// ---------- 开发期调试：hex dump ----------
/**
 * @brief 输出 `log_hex` 对应日志。
 * @param tag 标签值。
 * @param p 字节指针。
 * @param n 字节数。
 * @param max 最大输出长度。
 */
inline void log_hex(const char* tag, const uint8_t* p, size_t n, size_t max = 160) {
  const size_t m = (n > max) ? max : n;
  constexpr size_t kPerLine = 16;

  LOGI("%s: len=%zu (show=%zu)", tag, n, m);
  for (size_t i = 0; i < m; i += kPerLine) {
    char line[256];
    size_t off = 0;
    off += std::snprintf(line + off, sizeof(line) - off, "  %04zx: ", i);
    for (size_t j = 0; j < kPerLine; ++j) {
      if (i + j < m) off += std::snprintf(line + off, sizeof(line) - off, "%02X ", p[i + j]);
      else           off += std::snprintf(line + off, sizeof(line) - off, "   ");
    }
    LOGI("%s", line);
  }
  if (n > max) LOGI("%s: ... truncated", tag);
}

/**
 * @brief 输出 `log_frame_brief` 对应日志。
 * @param dir 目录路径。
 * @param fr 协议帧。
 */
inline void log_frame_brief(const char* dir, const btt::proto::Frame& fr) {
  LOGI("%s frame: cmd=0x%02X level=0x%02X seq=%u payload_len=%zu", dir, fr.cmd, fr.level, fr.seq, fr.payload.size());
}

/**
 * @brief 输出 `log_frame_detail` 对应日志。
 * @param dir 目录路径。
 * @param fr 协议帧。
 */
inline void log_frame_detail(const char* dir, const btt::proto::Frame& fr) {
  log_frame_brief(dir, fr);
  if (!fr.raw.empty()) {
    log_hex((std::string(dir) + " RAW").c_str(), fr.raw.data(), fr.raw.size());
  } else {
    // RX 没有 raw 时，至少 dump payload
    if (!fr.payload.empty()) log_hex((std::string(dir) + " PAYLOAD").c_str(), fr.payload.data(), fr.payload.size());
  }
}

// 前置声明（M5 持久化依赖）
inline bool ensure_dir(const std::string& path);
inline uint16_t bitrate_unit_to_kbps(uint8_t unit);
inline bool WriteFileAtomically(const std::string& path, const uint8_t* data,
                                size_t size);

// ---------------- M5：持久化配置（/data/m5_cfg.bin） ----------------
static constexpr const char* kM5CfgPath = "/data/m5_cfg.bin";
static constexpr const char* kBootReconnectReasonPath =
    "/data/boot_reconnect_reason.bin";

/**
 * @brief 执行 `ip_be_to_string` 内部辅助逻辑。
 * @param ip_be 大端 IPv4 地址。
 * @return 返回生成的字符串结果。
 */
inline std::string ip_be_to_string(uint32_t ip_be) {
  const uint8_t a = uint8_t((ip_be >> 24) & 0xFF);
  const uint8_t b = uint8_t((ip_be >> 16) & 0xFF);
  const uint8_t c = uint8_t((ip_be >> 8) & 0xFF);
  const uint8_t d = uint8_t(ip_be & 0xFF);
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%u.%u.%u.%u", a, b, c, d);
  return std::string(buf);
}
// 读取网卡 IPv4 和 netmask（返回 BE 整数：192.168.1.1 => 0xC0A80101）
/**
 * @brief 获取 `get_ipv4_and_mask_host` 对应信息。
 * @param ifname 网络接口名。
 * @param ip_host 主机字节序 IP 输出参数。
 * @param mask_host 主机字节序掩码输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
static bool get_ipv4_and_mask_host(const char* ifname, uint32_t& ip_host, uint32_t& mask_host) {
  int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) return false;

  struct ifreq ifr;
  std::memset(&ifr, 0, sizeof(ifr));
  std::snprintf(ifr.ifr_name, sizeof(ifr.ifr_name), "%s", ifname);

  // IP
  if (::ioctl(fd, SIOCGIFADDR, &ifr) != 0) { ::close(fd); return false; }
  auto* sin = reinterpret_cast<sockaddr_in*>(&ifr.ifr_addr);
  ip_host = ntohl(sin->sin_addr.s_addr);

  // MASK
  if (::ioctl(fd, SIOCGIFNETMASK, &ifr) != 0) { ::close(fd); return false; }
  auto* min = reinterpret_cast<sockaddr_in*>(&ifr.ifr_netmask);
  mask_host = ntohl(min->sin_addr.s_addr);

  ::close(fd);
  return true;
}

// 从 /proc/net/route 读取默认网关（返回 BE 整数：192.168.1.1 => 0xC0A80101）
/**
 * @brief 获取 `get_default_gateway_host` 对应信息。
 * @param ifname 网络接口名。
 * @param gw_host 主机字节序网关输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
static bool get_default_gateway_host(const char* ifname, uint32_t& gw_host) {
  std::ifstream f("/proc/net/route");
  if (!f.is_open()) return false;

  std::string line;
  std::getline(f, line); // header
  while (std::getline(f, line)) {
    std::istringstream iss(line);
    std::string iface, dest_hex, gw_hex;
    unsigned flags = 0;

    // /proc/net/route: Iface Destination Gateway Flags RefCnt Use Metric Mask ...
    if (!(iss >> iface >> dest_hex >> gw_hex >> std::hex >> flags)) continue;
    if (iface != ifname) continue;
    if (dest_hex != "00000000") continue; // default route

    // gateway 值是 little-endian hex，转成 host-order
    uint32_t gw_le = static_cast<uint32_t>(std::stoul(gw_hex, nullptr, 16));
    gw_host = ntohl(gw_le);
    return true;
  }
  return false;
}
/**
 * @brief 设置 `set_ipv4_addr_be` 对应状态。
 * @param ifname 网络接口名。
 * @param ip_be 大端 IPv4 地址。
 * @param out_errno 错误码输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
static bool set_ipv4_addr_be(const char* ifname, uint32_t ip_be, int& out_errno) {
  int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) { out_errno = errno; return false; }

  struct ifreq ifr{};
  std::snprintf(ifr.ifr_name, sizeof(ifr.ifr_name), "%s", ifname);

  sockaddr_in sin{};
  sin.sin_family = AF_INET;
  // 用 inet_pton，避免 endian 坑
  auto ip_str = ip_be_to_string(ip_be);
  if (::inet_pton(AF_INET, ip_str.c_str(), &sin.sin_addr) != 1) {
    out_errno = EINVAL; ::close(fd); return false;
  }
  std::memcpy(&ifr.ifr_addr, &sin, sizeof(sin));

  if (::ioctl(fd, SIOCSIFADDR, &ifr) != 0) {
    out_errno = errno; ::close(fd); return false;
  }

  ::close(fd);
  return true;
}

/**
 * @brief 设置 `set_ipv4_netmask_be` 对应状态。
 * @param ifname 网络接口名。
 * @param mask_be 大端子网掩码。
 * @param out_errno 错误码输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
static bool set_ipv4_netmask_be(const char* ifname, uint32_t mask_be, int& out_errno) {
  int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) { out_errno = errno; return false; }

  struct ifreq ifr{};
  std::snprintf(ifr.ifr_name, sizeof(ifr.ifr_name), "%s", ifname);

  sockaddr_in sin{};
  sin.sin_family = AF_INET;
  auto m_str = ip_be_to_string(mask_be);
  if (::inet_pton(AF_INET, m_str.c_str(), &sin.sin_addr) != 1) {
    out_errno = EINVAL; ::close(fd); return false;
  }
  std::memcpy(&ifr.ifr_netmask, &sin, sizeof(sin));

  if (::ioctl(fd, SIOCSIFNETMASK, &ifr) != 0) {
    out_errno = errno; ::close(fd); return false;
  }

  ::close(fd);
  return true;
}

/**
 * @brief 设置 `set_if_up` 对应状态。
 * @param ifname 网络接口名。
 * @param out_errno 错误码输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
static bool set_if_up(const char* ifname, int& out_errno) {
  int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) { out_errno = errno; return false; }

  struct ifreq ifr{};
  std::snprintf(ifr.ifr_name, sizeof(ifr.ifr_name), "%s", ifname);

  if (::ioctl(fd, SIOCGIFFLAGS, &ifr) != 0) {
    out_errno = errno; ::close(fd); return false;
  }

  ifr.ifr_flags |= (IFF_UP | IFF_RUNNING);
  if (::ioctl(fd, SIOCSIFFLAGS, &ifr) != 0) {
    out_errno = errno; ::close(fd); return false;
  }

  ::close(fd);
  return true;
}
/**
 * @brief 执行 `del_default_gateway_ioctl` 内部辅助逻辑。
 * @param ifname 网络接口名。
 * @param gw_be 大端网关地址。
 * @param out_errno 错误码输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
static bool del_default_gateway_ioctl(const char* ifname, uint32_t gw_be, int& out_errno) {
  int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) { out_errno = errno; return false; }

  struct rtentry rt{};
  // default dst = 0.0.0.0
  auto* dst = reinterpret_cast<sockaddr_in*>(&rt.rt_dst);
  dst->sin_family = AF_INET;
  dst->sin_addr.s_addr = INADDR_ANY;

  auto* gen = reinterpret_cast<sockaddr_in*>(&rt.rt_genmask);
  gen->sin_family = AF_INET;
  gen->sin_addr.s_addr = INADDR_ANY;

  // gateway 设为当前的 gw，有的系统 DELRT 需要匹配它
  auto* gw = reinterpret_cast<sockaddr_in*>(&rt.rt_gateway);
  gw->sin_family = AF_INET;
  auto gw_str = ip_be_to_string(gw_be);
  ::inet_pton(AF_INET, gw_str.c_str(), &gw->sin_addr);

  rt.rt_flags = RTF_UP | RTF_GATEWAY;
  rt.rt_dev = const_cast<char*>(ifname);

  if (::ioctl(fd, SIOCDELRT, &rt) != 0) {
    // 没有默认路由时可能报 ESRCH/EINVAL，通常可忽略
    out_errno = errno;
    ::close(fd);
    return false;
  }

  ::close(fd);
  return true;
}

/**
 * @brief 执行 `add_default_gateway_ioctl` 内部辅助逻辑。
 * @param ifname 网络接口名。
 * @param gw_be 大端网关地址。
 * @param out_errno 错误码输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
static bool add_default_gateway_ioctl(const char* ifname, uint32_t gw_be, int& out_errno) {
  int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) { out_errno = errno; return false; }

  struct rtentry rt{};

  auto* dst = reinterpret_cast<sockaddr_in*>(&rt.rt_dst);
  dst->sin_family = AF_INET;
  dst->sin_addr.s_addr = INADDR_ANY;

  auto* gen = reinterpret_cast<sockaddr_in*>(&rt.rt_genmask);
  gen->sin_family = AF_INET;
  gen->sin_addr.s_addr = INADDR_ANY;

  auto* gw = reinterpret_cast<sockaddr_in*>(&rt.rt_gateway);
  gw->sin_family = AF_INET;
  auto gw_str = ip_be_to_string(gw_be);
  if (::inet_pton(AF_INET, gw_str.c_str(), &gw->sin_addr) != 1) {
    out_errno = EINVAL; ::close(fd); return false;
  }

  rt.rt_flags = RTF_UP | RTF_GATEWAY;
  rt.rt_metric = 0;
  rt.rt_dev = const_cast<char*>(ifname);

  if (::ioctl(fd, SIOCADDRT, &rt) != 0) {
    out_errno = errno; ::close(fd); return false;
  }

  ::close(fd);
  return true;
}
/**
 * @brief 应用 `apply_net_config_ioctl` 对应配置。
 * @param ifname 网络接口名。
 * @param ip_be 大端 IPv4 地址。
 * @param mask_be 大端子网掩码。
 * @param gw_be 大端网关地址。
 * @param err `err` 参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
static bool apply_net_config_ioctl(const char* ifname, uint32_t ip_be, uint32_t mask_be, uint32_t gw_be,
                                   std::string& err) {
  int e = 0;
  uint32_t cur_ip_be = 0;
  uint32_t cur_mask_be = 0;
  const bool has_ip_mask = get_ipv4_and_mask_host(ifname, cur_ip_be, cur_mask_be);

  // 1) set ip（同值则跳过，避免重复下发）
  if (!has_ip_mask || cur_ip_be != ip_be) {
    if (!set_ipv4_addr_be(ifname, ip_be, e)) {
      err = "SIOCSIFADDR failed errno=" + std::to_string(e);
      return false;
    }
  }

  // 2) set netmask（同值则跳过，避免重复下发）
  if (!has_ip_mask || cur_mask_be != mask_be) {
    if (!set_ipv4_netmask_be(ifname, mask_be, e)) {
      err = "SIOCSIFNETMASK failed errno=" + std::to_string(e);
      return false;
    }
  }

  // 3) set if up
  (void)set_if_up(ifname, e); // up 失败不一定致命

  // 4) replace default gateway
  uint32_t old_gw_be = 0;
  const bool has_old_gw = get_default_gateway_host(ifname, old_gw_be);
  // 同值则无需动路由，视为成功（幂等）
  if (has_old_gw && old_gw_be == gw_be) return true;

  // 先尝试删除旧默认路由（如果有）
  if (has_old_gw) {
    int e2 = 0;
    (void)del_default_gateway_ioctl(ifname, old_gw_be, e2);
  }

  if (!add_default_gateway_ioctl(ifname, gw_be, e)) {
    // 路由已存在时，若默认网关已是目标值，视为成功（幂等）
    if (e == EEXIST) {
      uint32_t now_gw_be = 0;
      if (get_default_gateway_host(ifname, now_gw_be) && now_gw_be == gw_be) {
        return true;
      }
    }
    err = "SIOCADDRT(add default gw) failed errno=" + std::to_string(e);
    return false;
  }

  return true;
}
#pragma pack(push, 1)
struct M5PersistV1 {
  char magic[4]; // 'M''5''C''1'
  uint8_t ver;
  uint8_t rsv0[3];

  uint32_t dev_ip_be;
  uint32_t gateway_be;
  uint32_t netmask_be;
  uint32_t host_ip_be;
  uint16_t host_port;
  uint32_t upgrade_ip_be;
  uint32_t debug_ip_be;
  uint16_t debug_port;
  uint8_t  debug_proto;
  uint8_t  rsv1;

  uint8_t pic_res;
  uint8_t pic_quality;
  uint8_t video_res;
  uint8_t video_fps;
  uint8_t video_bitrate_unit;
  uint8_t rsv2[3];

  uint8_t measure_interval_min;
  uint8_t rsv3[3];

  // ---------------- M8：触发方式/事件策略（追加字段，兼容旧版文件） ----------------
  uint8_t m8_switch_trigger;
  uint8_t m8_pass_trigger;
  uint8_t rsv4[2];

  // 0x28 原始策略配置（2 份：event_type=0/1），固定存满 73B（查询时按 N 截断回显）
  uint8_t m8_strategy_raw0[73];
  uint8_t m8_strategy_raw1[73];
};
#pragma pack(pop)



/**
 * @brief 执行 `m5_read_persist` 相关内部逻辑。
 * @param out 输出对象。
 * @param out_bytes_read 读取字节数输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool m5_read_persist(M5PersistV1& out, size_t* out_bytes_read = nullptr) {
  std::memset(&out, 0, sizeof(out));
  int fd = ::open(kM5CfgPath, O_RDONLY);
  if (fd < 0) return false;

  struct stat st{};
  if (::fstat(fd, &st) != 0) { ::close(fd); return false; }
  size_t need = (st.st_size > 0) ? (size_t)st.st_size : 0;
  if (need < 8) { ::close(fd); return false; } // magic(4)+ver(1)+rsv0(3)
  if (need > sizeof(M5PersistV1)) need = sizeof(M5PersistV1);

  size_t off = 0;
  uint8_t* p = reinterpret_cast<uint8_t*>(&out);
  while (off < need) {
    ssize_t r = ::read(fd, p + off, need - off);
    if (r < 0) { if (errno == EINTR) continue; ::close(fd); return false; }
    if (r == 0) break;
    off += size_t(r);
  }
  ::close(fd);
  if (out_bytes_read) *out_bytes_read = off;

  // 兼容旧版：只要读到最小长度即可（后续新增字段保持 0）
  const size_t min_need = offsetof(M5PersistV1, m8_switch_trigger);
  if (off < min_need) return false;
  if (!(out.magic[0]=='M' && out.magic[1]=='5' && out.magic[2]=='C' && out.magic[3]=='1')) return false;
  if (out.ver != 1) return false;
  return true;
}

/**
 * @brief 执行 `m5_write_persist` 相关内部逻辑。
 * @param in `in` 参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool m5_write_persist(const M5PersistV1& in) {
  // 确保 /data 存在
  (void)ensure_dir("/data");
  return WriteFileAtomically(
      kM5CfgPath, reinterpret_cast<const uint8_t*>(&in), sizeof(M5PersistV1));
}

/**
 * @brief 持久化一次性启动重连原因，供重启后的首轮认证使用。
 * @param reason 待持久化的重连原因。
 * @return `true` 表示写入成功，`false` 表示写入失败。
 */
inline bool save_boot_reconnect_reason(uint8_t reason) {
  (void)ensure_dir("/data");
  return WriteFileAtomically(kBootReconnectReasonPath, &reason, sizeof(reason));
}

/**
 * @brief 读取一次性启动重连原因。
 * @param out_reason 输出参数。
 * @return `true` 表示读取到有效原因，`false` 表示无可用原因。
 */
inline bool load_boot_reconnect_reason(uint8_t& out_reason) {
  out_reason = 0;
  int fd = ::open(kBootReconnectReasonPath, O_RDONLY);
  if (fd < 0) return false;

  uint8_t reason = 0;
  size_t off = 0;
  while (off < sizeof(reason)) {
    const ssize_t r = ::read(fd, &reason + off, sizeof(reason) - off);
    if (r < 0) {
      if (errno == EINTR) continue;
      ::close(fd);
      return false;
    }
    if (r == 0) break;
    off += static_cast<size_t>(r);
  }
  ::close(fd);
  if (off != sizeof(reason)) return false;
  out_reason = reason;
  return true;
}

/**
 * @brief 删除一次性启动重连原因文件。
 */
inline void clear_boot_reconnect_reason() {
  if (::unlink(kBootReconnectReasonPath) != 0 && errno != ENOENT) {
    LOGW("clear boot reconnect reason failed errno=%d", errno);
  }
}

static constexpr uint16_t kM8DelayMaxMs = 500;
static constexpr uint16_t kM8VideoMaxDurMaxS = 15;
static constexpr uint16_t kM8TimeoutDefaultS = 60;

/**
 * @brief 执行 `u16_be` 内部辅助逻辑。
 * @param p 字节指针。
 * @return 返回计算结果。
 */
inline uint16_t u16_be(const uint8_t* p) {
  return uint16_t((uint16_t(p[0]) << 8) | uint16_t(p[1]));
}
/**
 * @brief 执行 `w16_be` 内部辅助逻辑。
 * @param p 字节指针。
 * @param v 输入数值。
 */
inline void w16_be(uint8_t* p, uint16_t v) {
  p[0] = uint8_t((v >> 8) & 0xFF);
  p[1] = uint8_t(v & 0xFF);
}

/**
 * @brief 执行 `m8_fill_default_strategy_raw` 相关内部逻辑。
 * @param event_type 事件类型。
 * @param out73 73 字节输出缓冲区。
 */
inline void m8_fill_default_strategy_raw(uint8_t event_type, uint8_t out73[73]) {
  std::memset(out73, 0, 73);
  out73[0] = event_type;

  // timeout_s (offset 15..16)
  w16_be(&out73[15], kM8TimeoutDefaultS);

  // item_count=2（video + pic）
  out73[24] = 2;

  // item0: video
  size_t o = 25;
  out73[o + 0] = 1;               // file_type=video
  w16_be(&out73[o + 1], 0);       // start_delay_ms
  w16_be(&out73[o + 3], 0);       // stop_delay_ms
  w16_be(&out73[o + 5], 15);      // max_dur_s

  // item1: pic
  o += 12;
  out73[o + 0] = 0;               // file_type=pic
  w16_be(&out73[o + 1], 0);       // start_delay_ms (用于结束后延时拍照)
  w16_be(&out73[o + 3], 0);
  w16_be(&out73[o + 5], 0);
}

/**
 * @brief 执行 `m8_apply_effective_from_strategy_raw` 相关内部逻辑。
 * @param event_type 事件类型。
 * @param raw73 73 字节原始策略数据。
 * @param rt 共享运行态。
 */
inline void m8_apply_effective_from_strategy_raw(uint8_t event_type, const uint8_t raw73[73], RuntimeConfig& rt) {
  const uint16_t timeout_s = u16_be(&raw73[15]);
  uint16_t out_timeout = timeout_s ? timeout_s : kM8TimeoutDefaultS;

  uint16_t v_start = 0, v_stop = 0, v_max_s = 15;
  uint16_t pic_delay = 0;
  bool have_v = false;
  bool have_pic = false;

  const uint8_t n = raw73[24];
  const uint8_t cnt = (n > 4) ? 4 : n;
  for (uint8_t i = 0; i < cnt; ++i) {
    const size_t o = 25 + size_t(i) * 12;
    const uint8_t ft = raw73[o + 0];
    const uint16_t sd = u16_be(&raw73[o + 1]);
    const uint16_t ed = u16_be(&raw73[o + 3]);
    const uint16_t md = u16_be(&raw73[o + 5]);

    if (ft == 1 && !have_v) {
      v_start = (sd <= kM8DelayMaxMs) ? sd : 0;
      v_stop  = (ed <= kM8DelayMaxMs) ? ed : 0;
      if (md == 0) v_max_s = 15;
      else v_max_s = (md <= kM8VideoMaxDurMaxS) ? md : 15;
      have_v = true;
    } else if (ft == 0 && !have_pic) {
      pic_delay = (sd <= kM8DelayMaxMs) ? sd : 0;
      have_pic = true;
    }
  }

  if (!have_v) { v_start = 0; v_stop = 0; v_max_s = 15; }
  if (!have_pic) { pic_delay = 0; }

  if (event_type == 0) {
    rt.m8_timeout_s0.store(out_timeout);
    rt.m8_v_start_delay_ms0.store(v_start);
    rt.m8_v_stop_delay_ms0.store(v_stop);
    rt.m8_v_max_dur_s0.store(v_max_s);
    rt.m8_pic_delay_ms0.store(pic_delay);
  } else {
    rt.m8_timeout_s1.store(out_timeout);
    rt.m8_v_start_delay_ms1.store(v_start);
    rt.m8_v_stop_delay_ms1.store(v_stop);
    rt.m8_v_max_dur_s1.store(v_max_s);
    rt.m8_pic_delay_ms1.store(pic_delay);
  }
}

/**
 * @brief 执行 `m5_load_into_runtime` 相关内部逻辑。
 * @param def 启动默认配置。
 * @param rt 共享运行态。
 */
inline void m5_load_into_runtime(const DefaultConfig& def, RuntimeConfig& rt) {
  M5PersistV1 cfg{};
  size_t bytes_read = 0;
  if (m5_read_persist(cfg, &bytes_read)) {
    rt.m5_dev_ip_be.store(cfg.dev_ip_be);
    rt.m5_gateway_be.store(cfg.gateway_be);
    rt.m5_netmask_be.store(cfg.netmask_be);
    rt.m5_host_ip_be.store(cfg.host_ip_be);
    rt.m5_host_port.store(cfg.host_port);
    rt.m5_upgrade_server_ip_be.store(cfg.upgrade_ip_be);
    rt.m5_debug_server_ip_be.store(cfg.debug_ip_be);
    rt.m5_debug_server_port.store(cfg.debug_port);
    rt.m5_debug_proto.store(cfg.debug_proto);

    rt.m5_pic_res.store(cfg.pic_res);
    rt.m5_pic_quality.store(cfg.pic_quality);
    rt.m5_video_res.store(cfg.video_res);
    rt.m5_video_fps.store(cfg.video_fps);
    rt.m5_video_bitrate_unit.store(cfg.video_bitrate_unit);
    rt.video_bitrate_kbps.store(bitrate_unit_to_kbps(cfg.video_bitrate_unit));

    rt.m5_measure_interval_min.store(cfg.measure_interval_min);

    // M8：兼容旧版文件：若未包含追加字段，则使用默认值
    const size_t min_with_m8 = offsetof(M5PersistV1, m8_switch_trigger) + 2;
    if (bytes_read >= min_with_m8) {
      rt.m8_switch_trigger.store(cfg.m8_switch_trigger);
      rt.m8_pass_trigger.store(cfg.m8_pass_trigger);

      {
        std::lock_guard<std::mutex> lk(rt.m8_mu);
        std::memcpy(rt.m8_strategy_raw0.data(), cfg.m8_strategy_raw0, 73);
        std::memcpy(rt.m8_strategy_raw1.data(), cfg.m8_strategy_raw1, 73);
      }

      m8_apply_effective_from_strategy_raw(0, cfg.m8_strategy_raw0, rt);
      m8_apply_effective_from_strategy_raw(1, cfg.m8_strategy_raw1, rt);
    } else {
      // 默认：TCP 触发 + 默认策略
      rt.m8_switch_trigger.store(1);
      rt.m8_pass_trigger.store(1);

      uint8_t raw0[73]{}, raw1[73]{};
      m8_fill_default_strategy_raw(0, raw0);
      m8_fill_default_strategy_raw(1, raw1);
      {
        std::lock_guard<std::mutex> lk(rt.m8_mu);
        std::memcpy(rt.m8_strategy_raw0.data(), raw0, 73);
        std::memcpy(rt.m8_strategy_raw1.data(), raw1, 73);
      }
      m8_apply_effective_from_strategy_raw(0, raw0, rt);
      m8_apply_effective_from_strategy_raw(1, raw1, rt);
    }
  }

  // host 默认回落到 def.server_ip/port
  if (rt.m5_host_ip_be.load() == 0) {
    // 简易 inet_pton：使用系统库
    in_addr a{};
    if (::inet_pton(AF_INET, def.server_ip.c_str(), &a) == 1) {
      rt.m5_host_ip_be.store(ntohl(a.s_addr)); // ntohl -> BE 整数形式
    }
  }
  if (rt.m5_host_port.load() == 0) rt.m5_host_port.store(def.server_port);

  // 设备侧网络参数默认回落到出厂值，避免首次启动/无持久化时 eth0 不被配置
  if (rt.m5_dev_ip_be.load() == 0) rt.m5_dev_ip_be.store(kFactoryDevIpBe);
  if (rt.m5_gateway_be.load() == 0) rt.m5_gateway_be.store(kFactoryGatewayIpBe);
  if (rt.m5_netmask_be.load() == 0) rt.m5_netmask_be.store(kFactoryNetmaskBe);

  // media 默认回落
  if (rt.m5_video_bitrate_unit.load() == 0) {
    rt.m5_video_bitrate_unit.store(kFactoryMediaVideoBitrateUnit);
  }
  if (rt.video_bitrate_kbps.load() == 0) {
    rt.video_bitrate_kbps.store(
        bitrate_unit_to_kbps(rt.m5_video_bitrate_unit.load()));
  }
  if (rt.m5_video_fps.load() == 0) rt.m5_video_fps.store(25);
  if (rt.m5_pic_quality.load() == 0) rt.m5_pic_quality.store(75);

  // periodic 默认关闭
  rt.m5_measure_periodic.store(false);
  if (rt.m5_measure_interval_min.load() == 0) rt.m5_measure_interval_min.store(30);
}

/**
 * @brief 执行 `m5_apply_persisted_eth0_if_ready` 相关内部逻辑。
 * @param rt 共享运行态。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool m5_apply_persisted_eth0_if_ready(const RuntimeConfig& rt) {
  const uint32_t dev_ip = rt.m5_dev_ip_be.load();
  const uint32_t gw = rt.m5_gateway_be.load();
  const uint32_t mask = rt.m5_netmask_be.load();
  if (dev_ip == 0 || gw == 0 || mask == 0) {
    LOGI("M5 BOOT NET skip: persisted eth0 config incomplete dev=%s gw=%s mask=%s",
         ip_be_to_string(dev_ip).c_str(),
         ip_be_to_string(gw).c_str(),
         ip_be_to_string(mask).c_str());
    return false;
  }

  std::string err;
  if (!apply_net_config_ioctl("eth0", dev_ip, mask, gw, err)) {
    LOGW("M5 BOOT NET apply failed: %s", err.c_str());
    return false;
  }

  LOGI("M5 BOOT NET applied: dev=%s gw=%s mask=%s",
       ip_be_to_string(dev_ip).c_str(),
       ip_be_to_string(gw).c_str(),
       ip_be_to_string(mask).c_str());
  return true;
}

/**
 * @brief 执行 `m5_snapshot_runtime_to_persist` 相关内部逻辑。
 * @param rt 共享运行态。
 * @param out 输出对象。
 */
inline void m5_snapshot_runtime_to_persist(const RuntimeConfig& rt, M5PersistV1& out) {
  out.magic[0]='M'; out.magic[1]='5'; out.magic[2]='C'; out.magic[3]='1';
  out.ver = 1;
  out.rsv0[0]=out.rsv0[1]=out.rsv0[2]=0;

  out.dev_ip_be = rt.m5_dev_ip_be.load();
  out.gateway_be = rt.m5_gateway_be.load();
  out.netmask_be = rt.m5_netmask_be.load();
  out.host_ip_be = rt.m5_host_ip_be.load();
  out.host_port = rt.m5_host_port.load();
  out.upgrade_ip_be = rt.m5_upgrade_server_ip_be.load();
  out.debug_ip_be = rt.m5_debug_server_ip_be.load();
  out.debug_port = rt.m5_debug_server_port.load();
  out.debug_proto = rt.m5_debug_proto.load();
  out.rsv1 = 0;

  out.pic_res = rt.m5_pic_res.load();
  out.pic_quality = rt.m5_pic_quality.load();
  out.video_res = rt.m5_video_res.load();
  out.video_fps = rt.m5_video_fps.load();
  out.video_bitrate_unit = rt.m5_video_bitrate_unit.load();
  out.rsv2[0]=out.rsv2[1]=out.rsv2[2]=0;

  out.measure_interval_min = rt.m5_measure_interval_min.load();
  out.rsv3[0]=out.rsv3[1]=out.rsv3[2]=0;

  // M8：触发方式/策略
  out.m8_switch_trigger = rt.m8_switch_trigger.load();
  out.m8_pass_trigger = rt.m8_pass_trigger.load();
  out.rsv4[0] = 0;
  out.rsv4[1] = 0;

  std::array<uint8_t, 73> r0{}, r1{};
  {
    std::lock_guard<std::mutex> lk(rt.m8_mu);
    r0 = rt.m8_strategy_raw0;
    r1 = rt.m8_strategy_raw1;
  }
  std::memcpy(out.m8_strategy_raw0, r0.data(), 73);
  std::memcpy(out.m8_strategy_raw1, r1.data(), 73);
}

/**
 * @brief 执行 `m5_persist_save` 相关内部逻辑。
 * @param rt 共享运行态。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool m5_persist_save(const RuntimeConfig& rt) {
  M5PersistV1 cfg{};
  m5_snapshot_runtime_to_persist(rt, cfg);
  return m5_write_persist(cfg);
}

/**
 * @brief 执行 `bitrate_unit_to_kbps` 内部辅助逻辑。
 * @param unit 码率单位值。
 * @return 返回计算结果。
 */
inline uint16_t bitrate_unit_to_kbps(uint8_t unit) {
  if (unit == 17) return 560;
  return uint16_t(unit) * 32u;
}


// ---------- BCD 时间同步（用于认证/心跳应答）----------
/**
 * @brief 将 `from_bcd` 输入转换为数值。
 * @param x0F `x0F` 参数。
 * @return 返回计算结果。
 */
inline int from_bcd(uint8_t b) { return ((b >> 4) & 0x0F) * 10 + (b & 0x0F); }
/**
 * @brief 将 `from_bcd16` 输入转换为数值。
 * @param b BCD 输入值。
 * @return 返回计算结果。
 */
inline int from_bcd16(uint16_t b) {
  int d3 = (b >> 12) & 0x0F;
  int d2 = (b >> 8)  & 0x0F;
  int d1 = (b >> 4)  & 0x0F;
  int d0 = (b >> 0)  & 0x0F;
  return d3 * 1000 + d2 * 100 + d1 * 10 + d0;
}

/**
 * @brief 同步 `sync_system_time_from_bcd8` 对应状态。
 * @param t8 8 字节 BCD 时间。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool sync_system_time_from_bcd8(const uint8_t* t8) {
  int yy = from_bcd(t8[0]);
  int mo = from_bcd(t8[1]);
  int dd = from_bcd(t8[2]);
  int hh = from_bcd(t8[3]);
  int mi = from_bcd(t8[4]);
  int ss = from_bcd(t8[5]);
  int ms = from_bcd16(uint16_t(t8[6] << 8) | t8[7]);

  std::tm tm{};
  tm.tm_year = (2000 + yy) - 1900;
  tm.tm_mon  = mo - 1;
  tm.tm_mday = dd;
  tm.tm_hour = hh;
  tm.tm_min  = mi;
  tm.tm_sec  = ss;

  time_t tt = ::mktime(&tm);
  if (tt == (time_t)-1) return false;

  timespec ts{};
  ts.tv_sec = tt;
  ts.tv_nsec = ms * 1000000L;
  return ::clock_settime(CLOCK_REALTIME, &ts) == 0;
}

// ---------- 认证/心跳组包 ----------
/**
 * @brief 构造 `make_auth_req` 对应结果。
 * @param def 启动默认配置。
 * @param rt 共享运行态。
 * @return 返回构造后的协议帧。
 */
inline btt::proto::Frame make_auth_req(const DefaultConfig& def, RuntimeConfig& rt) {
  btt::proto::Frame f;
  f.cmd = 0x00;
  f.level = 0x00; // 设备->上位机 请求
  f.seq = rt.seq.fetch_add(1);

  // 14B: SN(5)+VER(3)+MODEL(1)+HB(1)+RECONN(1)+REASON(1)+MODE(1)+POS(1)
  f.payload.resize(14);
  size_t o = 0;

  f.payload[o++] = 0x01;
  f.payload[o++] = 0x18;
  f.payload[o++] = 0x0A;
  f.payload[o++] = 0x0B;
  f.payload[o++] = 0x8F;

  f.payload[o++] = def.ver_major;
  f.payload[o++] = def.ver_minor;
  f.payload[o++] = def.ver_tag;

  f.payload[o++] = rt.switch_model.load();
  f.payload[o++] = rt.hb_interval_s.load();
  f.payload[o++] = rt.reconnect_interval_s.load();
  f.payload[o++] = rt.reconnect_reason.load();
  f.payload[o++] = def.run_mode;
  f.payload[o++] = rt.position.load();

  return f;
}

/**
 * @brief 构造 `make_heartbeat_req` 对应结果。
 * @param rt 共享运行态。
 * @return 返回构造后的协议帧。
 */
inline btt::proto::Frame make_heartbeat_req(RuntimeConfig& rt) {
  btt::proto::Frame f;
  f.cmd = 0x01;
  f.level = 0x00; // 设备->上位机 请求
  f.seq = rt.seq.fetch_add(1);

  // 13B: TIME(8)+DEV_STATE(4)+POS(1)
  std::vector<uint8_t> t8;
  btt::proto::build_bcdtime8(t8);

  f.payload.resize(13);
  size_t o = 0;
  std::memcpy(&f.payload[o], t8.data(), 8); o += 8;

  f.payload[o++] = 0x00; f.payload[o++] = 0x00; f.payload[o++] = 0x00; f.payload[o++] = 0x00;
  f.payload[o++] = rt.position.load();
  return f;
}

// ---------- 认证/心跳应答处理（设备收到服务器发来的 0x00/0x01） ----------
/**
 * @brief 处理 `handle_auth_resp` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 */
inline void handle_auth_resp(const btt::proto::Frame& fr, RuntimeConfig& rt) {
  if (fr.payload.size() < 14) {
    LOGW("AUTH RESP payload too short: %zu", fr.payload.size());
    return;
  }

  const uint8_t result = fr.payload[0];
  const uint8_t reason = fr.payload[1];

  (void)sync_system_time_from_bcd8(&fr.payload[2]);

  if (result == 0x00) {
    const uint8_t hb  = fr.payload[10];
    const uint8_t ri  = fr.payload[11];
    const uint8_t nc  = fr.payload[12];
    const uint8_t pos = fr.payload[13];

    rt.hb_interval_s.store(hb);
    rt.reconnect_interval_s.store(ri);
    rt.no_comm_reboot_s.store(nc);
    rt.position.store(pos);

    rt.authed.store(true);
    rt.hb_miss.store(0);

    LOGI("AUTH OK: hb=%u reconn=%u no_comm=%u pos=%u", hb, ri, nc, pos);
  } else {
    rt.authed.store(false);
    rt.reconnect_reason.store(19); // 认证失败
    LOGW("AUTH FAIL: reason=0x%02X", reason);

    if (reason == 0x01 || reason == 0x02 || reason == 0x04) {
      rt.reconnect_disabled.store(true);
    }
  }
}

/**
 * @brief 处理 `handle_heartbeat_resp` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 */
inline void handle_heartbeat_resp(const btt::proto::Frame& fr, RuntimeConfig& rt) {
  if (fr.payload.size() < 8) {
    LOGW("HB RESP payload too short: %zu", fr.payload.size());
    return;
  }
  (void)sync_system_time_from_bcd8(&fr.payload[0]);
  rt.hb_miss.store(0);
}

/**
 * @brief 构造升级结果事件 `0x58`。
 * @param rt 共享运行态。
 * @param report_result 守护进程持久化的升级结果。
 * @param report_reason 守护进程持久化的升级结果原因码。
 * @return 返回待发送的事件帧。
 */
inline btt::proto::Frame make_upgrade_result_58_req(RuntimeConfig& rt,
                                                    uint8_t report_result,
                                                    uint8_t report_reason) {
  btt::proto::Frame f;
  f.cmd = 0x58;
  f.level = 0x00;
  f.seq = rt.seq.fetch_add(1);
  f.payload = {0x00, report_result, report_reason};
  return f;
}

/**
 * @brief 处理 `0x58` 上位机 ACK。
 * @param fr 协议帧。
 * @param ctl_client 升级控制客户端。
 */
inline void handle_upgrade_result_58_resp(
    const btt::proto::Frame& fr, UpgradedControlClient& ctl_client) {
  if (fr.payload.size() < 2) {
    LOGW("0x58 ACK payload too short: %zu", fr.payload.size());
    return;
  }
  const uint8_t result = fr.payload[0];
  const uint8_t reason = fr.payload[1];
  if (result != 0x00) {
    LOGW("0x58 ACK failed: result=%u reason=0x%02X", result, reason);
    return;
  }

  upgraded::UpgradeCtlResponseV1 response{};
  if (!ctl_client.ClearReport(static_cast<uint32_t>(::getpid()), &response)) {
    LOGW("0x58 ACK clear report rpc failed");
    return;
  }
  if (response.result != upgraded::kUpgradeCtlResultOk) {
    LOGW("0x58 ACK clear report rejected reason=0x%02X", response.reason);
    return;
  }
  LOGI("0x58 ACK accepted, clear report pending");
}

/**
 * @brief 在认证成功后查询并发送待上报的升级结果。
 * @param rt 共享运行态。
 * @param ctl_client 升级控制客户端。
 * @param outbound 输出队列。
 * @return 本次是否成功入队 `0x58` 结果事件。
 */
inline bool maybe_send_upgrade_result_after_auth(
    RuntimeConfig& rt, UpgradedControlClient& ctl_client,
    btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  upgraded::UpgradeCtlResponseV1 response{};
  if (!ctl_client.QueryReport(static_cast<uint32_t>(::getpid()), &response)) {
    LOGW("query upgrade report rpc failed after auth");
    return false;
  }
  if (response.result != upgraded::kUpgradeCtlResultOk) {
    LOGW("query upgrade report rejected reason=0x%02X", response.reason);
    return false;
  }
  if (response.report_pending == 0) return false;

  LOGI("queue 0x58 upgrade result result=%u reason=0x%02X phase=%u",
       static_cast<unsigned>(response.report_result),
       static_cast<unsigned>(response.report_reason),
       static_cast<unsigned>(response.upgrade_phase));
  outbound.push(make_upgrade_result_58_req(rt, response.report_result,
                                           response.report_reason));
  return true;
}

/**
 * @brief 判断升级静默模式下是否应拒绝某个业务命令。
 * @param cmd 功能码。
 * @return 需要拒绝时返回 `true`。
 */
inline bool should_reject_cmd_during_upgrade_quiet(uint8_t cmd) {
  switch (cmd) {
    case 0x30:
    case 0x31:
    case 0x33:
    case 0x35:
    case 0x36:
    case 0x38:
    case 0x39:
    case 0x3A:
    case 0x41:
    case 0xA6:
      return true;
    default:
      return false;
  }
}

// ---------- M2.1：0x20/0x21 设备配置 ----------
static constexpr size_t kDevCfgLen = 36;

// 36B 偏移定义
namespace devcfg_off {
  static constexpr size_t kSwitchModel         = 0;
  static constexpr size_t kOilPressureEn       = 1;
  static constexpr size_t kLevelSensorEn       = 2;
  static constexpr size_t kAccelEn             = 3;
  static constexpr size_t kCurrentEn           = 4;
  static constexpr size_t kCamCount            = 5;

  static constexpr size_t kCam1Id              = 6;
  static constexpr size_t kCam1Pos             = 7;
  static constexpr size_t kCam2Id              = 8;
  static constexpr size_t kCam2Pos             = 9;
  static constexpr size_t kCam3Id              = 10;
  static constexpr size_t kCam3Pos             = 11;
  static constexpr size_t kCam4Id              = 12;
  static constexpr size_t kCam4Pos             = 13;

  static constexpr size_t kHbIntervalS         = 14;
  static constexpr size_t kBusinessTimeoutS    = 15;
  static constexpr size_t kReconnectIntervalS  = 16;
  static constexpr size_t kNoCommRebootS       = 17;

  static constexpr size_t kDefaultGap          = 18;
  static constexpr size_t kCtWiring            = 19;
  static constexpr size_t kCtPos               = 20;
  static constexpr size_t kOilPressurePos      = 21;
  static constexpr size_t kLevelComp           = 22;
  static constexpr size_t kCtCompMode          = 23;
  static constexpr size_t kCtCompValueHi       = 24;
  static constexpr size_t kCtCompValueLo       = 25;
  static constexpr size_t kSolenoidCompMode    = 26;
  static constexpr size_t kSolenoidCompValueHi = 27;
  static constexpr size_t kSolenoidCompValueLo = 28;
  static constexpr size_t kLampCount           = 29;
  static constexpr size_t kLampCh0             = 30;
  static constexpr size_t kLampCh1             = 31;
  static constexpr size_t kLampCh2             = 32;
  static constexpr size_t kLampCh3             = 33;
  static constexpr size_t kReserved0           = 34;
  static constexpr size_t kReserved1           = 35;
}

/**
 * @brief 输出 `log_devcfg_fields` 对应日志。
 * @param c 配置字节数组。
 */
inline void log_devcfg_fields(const std::array<uint8_t, kDevCfgLen>& c) {
  LOGI("DEVCFG36 parsed:");
  LOGI("  switch_model=%u", c[devcfg_off::kSwitchModel]);
  LOGI("  sensors: oil=%u level=%u accel=%u current=%u",
       c[devcfg_off::kOilPressureEn], c[devcfg_off::kLevelSensorEn],
       c[devcfg_off::kAccelEn], c[devcfg_off::kCurrentEn]);
  LOGI("  cam_count=%u", c[devcfg_off::kCamCount]);
  LOGI("  cam1: id=%u pos=0x%02X", c[devcfg_off::kCam1Id], c[devcfg_off::kCam1Pos]);
  LOGI("  cam2: id=%u pos=0x%02X", c[devcfg_off::kCam2Id], c[devcfg_off::kCam2Pos]);
  LOGI("  cam3: id=%u pos=0x%02X", c[devcfg_off::kCam3Id], c[devcfg_off::kCam3Pos]);
  LOGI("  cam4: id=%u pos=0x%02X", c[devcfg_off::kCam4Id], c[devcfg_off::kCam4Pos]);
  LOGI("  hb=%u biz_to=%u reconn=%u no_comm_reboot=%u",
       c[devcfg_off::kHbIntervalS], c[devcfg_off::kBusinessTimeoutS],
       c[devcfg_off::kReconnectIntervalS], c[devcfg_off::kNoCommRebootS]);
  LOGI("  default_gap=0x%02X ct_wiring=0x%02X ct_pos=0x%02X oil_pos=0x%02X level_comp=0x%02X",
       c[devcfg_off::kDefaultGap], c[devcfg_off::kCtWiring], c[devcfg_off::kCtPos],
       c[devcfg_off::kOilPressurePos], c[devcfg_off::kLevelComp]);
  LOGI("  ct_comp: mode=0x%02X value=0x%02X%02X",
       c[devcfg_off::kCtCompMode], c[devcfg_off::kCtCompValueHi], c[devcfg_off::kCtCompValueLo]);
  LOGI("  solenoid_comp: mode=0x%02X value=0x%02X%02X",
       c[devcfg_off::kSolenoidCompMode], c[devcfg_off::kSolenoidCompValueHi], c[devcfg_off::kSolenoidCompValueLo]);
  LOGI("  lamps: count=%u ch=[%02X %02X %02X %02X] reserved=[%02X %02X]",
       c[devcfg_off::kLampCount], c[devcfg_off::kLampCh0], c[devcfg_off::kLampCh1],
       c[devcfg_off::kLampCh2], c[devcfg_off::kLampCh3],
       c[devcfg_off::kReserved0], c[devcfg_off::kReserved1]);
}

// 默认36B配置（未收到0x20时用于0x21应答）
/**
 * @brief 构造 `make_default_devcfg` 对应结果。
 * @param rt 共享运行态。
 * @return 返回计算结果。
 */
inline std::array<uint8_t, kDevCfgLen> make_default_devcfg(RuntimeConfig& rt) {
  std::array<uint8_t, kDevCfgLen> c{};
  c[devcfg_off::kSwitchModel] = rt.switch_model.load();

  c[devcfg_off::kOilPressureEn] = 0x00;
  c[devcfg_off::kLevelSensorEn] = 0x00;
  c[devcfg_off::kAccelEn]       = 0x00;
  c[devcfg_off::kCurrentEn]     = 0x00;

  c[devcfg_off::kCamCount] = 0x04;
  c[devcfg_off::kCam1Id] = 0x00; c[devcfg_off::kCam1Pos] = 0x00;
  c[devcfg_off::kCam2Id] = 0x01; c[devcfg_off::kCam2Pos] = 0x00;
  c[devcfg_off::kCam3Id] = 0x02; c[devcfg_off::kCam3Pos] = 0x00;
  c[devcfg_off::kCam4Id] = 0x03; c[devcfg_off::kCam4Pos] = 0x00;

  c[devcfg_off::kHbIntervalS]        = rt.hb_interval_s.load();
  c[devcfg_off::kBusinessTimeoutS]   = rt.business_timeout_s.load();
  c[devcfg_off::kReconnectIntervalS] = rt.reconnect_interval_s.load();
  c[devcfg_off::kNoCommRebootS]      = rt.no_comm_reboot_s.load();

  // 18..35 默认 0
  return c;
}

/**
 * @brief 应用 `apply_devcfg_runtime` 对应配置。
 * @param c 配置字节数组。
 * @param rt 共享运行态。
 */
inline void apply_devcfg_runtime(const std::array<uint8_t, kDevCfgLen>& c, RuntimeConfig& rt) {
  rt.switch_model.store(c[devcfg_off::kSwitchModel]);
  rt.hb_interval_s.store(c[devcfg_off::kHbIntervalS]);
  rt.business_timeout_s.store(c[devcfg_off::kBusinessTimeoutS]);
  rt.reconnect_interval_s.store(c[devcfg_off::kReconnectIntervalS]);
  rt.no_comm_reboot_s.store(c[devcfg_off::kNoCommRebootS]);
}

/**
 * @brief 构造 `make_resp_2b` 对应结果。
 * @param cmd 协议命令字。
 * @param seq 协议序号。
 * @param result 结果码。
 * @param reason 原因码。
 * @return 返回构造后的协议帧。
 */
inline btt::proto::Frame make_resp_2b(uint8_t cmd, uint16_t seq, uint8_t result, uint8_t reason) {
  btt::proto::Frame r;
  r.cmd = cmd;
  r.level = 0x01; // 设备->上位机 应答
  r.seq = seq;
  r.payload = {result, reason};
  return r;
}

/**
 * @brief 判断 `is_bool01` 条件是否成立。
 * @param v 输入数值。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool is_bool01(uint8_t v) { return v == 0x00 || v == 0x01; }

/**
 * @brief 处理 `handle_set_devcfg_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param outbound 输出队列。
 */
inline void handle_set_devcfg_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                  btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  if (fr.payload.size() != kDevCfgLen) {
    LOGW("0x20 SET_DEVCFG: bad payload len=%zu (expect %zu)", fr.payload.size(), kDevCfgLen);
    result = 0x01;
    outbound.push(make_resp_2b(0x20, fr.seq, result, reason));
    return;
  }

  std::array<uint8_t, kDevCfgLen> c{};
  std::memcpy(c.data(), fr.payload.data(), kDevCfgLen);

  const uint8_t cam_cnt = c[devcfg_off::kCamCount];
  if (cam_cnt > 4) result = 0x01;

  if (!is_bool01(c[devcfg_off::kOilPressureEn]) ||
      !is_bool01(c[devcfg_off::kLevelSensorEn]) ||
      !is_bool01(c[devcfg_off::kAccelEn]) ||
      !is_bool01(c[devcfg_off::kCurrentEn])) {
    result = 0x01;
  }

  if (result == 0x00) {
    {
      std::lock_guard<std::mutex> lk(rt.devcfg_mu);
      rt.devcfg_raw = c;
      rt.devcfg_valid.store(true);
    }

    apply_devcfg_runtime(c, rt);

    LOGI("0x20 SET_DEVCFG OK (seq=%u)", fr.seq);
    log_devcfg_fields(c);
    log_hex("DEVCFG36 RAW", c.data(), c.size());
  } else {
    LOGW("0x20 SET_DEVCFG FAIL (seq=%u)", fr.seq);
    log_hex("DEVCFG36 RAW", c.data(), c.size());
  }

  outbound.push(make_resp_2b(0x20, fr.seq, result, reason));
}

/**
 * @brief 处理 `handle_get_devcfg_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param outbound 输出队列。
 */
inline void handle_get_devcfg_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                  btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;
  if (!fr.payload.empty()) {
    LOGW("0x21 GET_DEVCFG: payload not empty (len=%zu), ignore", fr.payload.size());
  }

  std::array<uint8_t, kDevCfgLen> c{};
  if (rt.devcfg_valid.load()) {
    std::lock_guard<std::mutex> lk(rt.devcfg_mu);
    c = rt.devcfg_raw;
  } else {
    c = make_default_devcfg(rt);
  }

  btt::proto::Frame resp;
  resp.cmd = 0x21;
  resp.level = 0x01; // 设备->上位机 应答
  resp.seq = fr.seq;
  resp.payload.reserve(2 + c.size());
  resp.payload.push_back(result);
  resp.payload.push_back(reason);
  resp.payload.insert(resp.payload.end(), c.begin(), c.end());

  LOGI("0x21 GET_DEVCFG RESP (seq=%u)", fr.seq);
  log_devcfg_fields(c);

  outbound.push(std::move(resp));
}

// ---------- M3：0x30(拍照测量) / 0x39(开始录像) / 0x3A(停止录像) ----------
// 说明：

// - 当前阶段：录像直接保存 H.264 裸流（不做 MP4 封装）

// 目录：/data/events/<event_id>/
static constexpr const char* kEventsDir = "/data/events";

/**
 * @brief 构造 `make_media_file_path_legacy` 对应结果。
 * @param dir 目录路径。
 * @param file_id 文件 ID。
 * @param ext 文件扩展名。
 * @return 返回生成的字符串结果。
 */
inline std::string make_media_file_path_legacy(const std::string& dir, uint32_t file_id, const char* ext) {
  char path[256];
  std::snprintf(path, sizeof(path), "%s/%08X.%s", dir.c_str(), file_id, ext);
  return std::string(path);
}

/**
 * @brief 构造 `make_media_file_path_v2` 对应结果。
 * @param dir 目录路径。
 * @param file_id 文件 ID。
 * @param ext 文件扩展名。
 * @return 返回生成的字符串结果。
 */
inline std::string make_media_file_path_v2(const std::string& dir, uint32_t file_id, const char* ext) {
  char path[256];
  std::snprintf(path, sizeof(path), "%s/F_%08X.%s", dir.c_str(), file_id, ext);
  return std::string(path);
}

/**
 * @brief 构造 `make_media_file_path_for_write` 对应结果。
 * @param dir 目录路径。
 * @param file_id 文件 ID。
 * @param ext 文件扩展名。
 * @return 返回生成的字符串结果。
 */
inline std::string make_media_file_path_for_write(const std::string& dir, uint32_t file_id, const char* ext) {
  // 新命名规则：F_<file_id>.<ext>
  return make_media_file_path_v2(dir, file_id, ext);
}

// ---- 存储策略（/data 专用于事件）----
static constexpr uint64_t kEventBudgetBytes  = 47ull * 1024 * 1024; // 事件可用预算（留出安全余量）
static constexpr uint64_t kReserveBytes      = 5ull  * 1024 * 1024; // /data 保底余量
static constexpr uint64_t kMetaMaxBytes      = 200ull;              // meta.bin 上限
static constexpr uint64_t kJpgMaxBytes       = 50ull * 1024;        // 单张 JPG 上限（模型上界）
static constexpr uint32_t kVideoMaxMs        = 15u  * 1000;         // 单路录像最大 15s
static constexpr uint32_t kEventMaxMs        = 60u  * 1000;         // 事件最大 60s（目前仅做基础框架，不主动应答）

/**
 * @brief 执行 `clamp_bitrate_kbps` 内部辅助逻辑。
 * @param kbps 码率，单位 kbps。
 * @return 返回计算结果。
 */
inline uint32_t clamp_bitrate_kbps(uint32_t kbps) { return (kbps > 1024u) ? 1024u : kbps; }

/**
 * @brief 映射 `map_res_code_to_hif` 对应数值。
 * @param code 枚举或配置编码值。
 * @param out_pic_size 图片分辨率输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool map_res_code_to_hif(uint8_t code, hif_pic_size& out_pic_size) {
  switch (code) {
    case 3: out_pic_size = HIF_PIC_D1_NTSC; return true;
    case 6: out_pic_size = HIF_PIC_D1_PAL;  return true;
    case 7: out_pic_size = HIF_PIC_720P;    return true;
    case 8: out_pic_size = HIF_PIC_1080P;   return true;
    default: return false;
  }
}

// 单路 15s 视频上界：bytes ≈ bitrate_kbps / 8 * 15 (KB) * 1024
/**
 * @brief 估算 `est_video15s_bytes` 对应大小。
 * @param bitrate_kbps `bitrate_kbps` 参数。
 * @return 返回计算结果。
 */
inline uint64_t est_video15s_bytes(uint32_t bitrate_kbps) {
  const uint32_t kbps = clamp_bitrate_kbps(bitrate_kbps);
  return (uint64_t)kbps * 15ull * 1024ull / 8ull;
}

// A 类：4 张照片
/**
 * @brief 估算 `est_event_A_bytes` 对应大小。
 * @return 返回计算结果。
 */
inline uint64_t est_event_A_bytes() {
  return kMetaMaxBytes + 4ull * kJpgMaxBytes;
}

// B 类：4 张照片 + 4 路 15s 视频
/**
 * @brief 估算 `est_event_B_bytes` 对应大小。
 * @param bitrate_kbps `bitrate_kbps` 参数。
 * @return 返回计算结果。
 */
inline uint64_t est_event_B_bytes(uint32_t bitrate_kbps) {
  return kMetaMaxBytes + 4ull * kJpgMaxBytes + 4ull * est_video15s_bytes(bitrate_kbps);
}

// 前置声明（供存储准入/清理使用）
inline bool is_hex8_dirname(const char* s);
inline uint32_t parse_hex8_u32(const char* s);
inline bool rm_rf(const std::string& path);
inline void cleanup_event_tmp_tar(uint32_t event_id);

/**
 * @brief 执行 `fs_free_bytes` 内部辅助逻辑。
 * @param path 路径。
 * @return 返回计算结果。
 */
inline uint64_t fs_free_bytes(const char* path) {
  struct statvfs v{};
  if (::statvfs(path, &v) != 0) return 0;
  return (uint64_t)v.f_bavail * (uint64_t)v.f_frsize;
}

/**
 * @brief 执行 `dir_size_bytes` 内部辅助逻辑。
 * @param path 路径。
 * @return 返回计算结果。
 */
inline uint64_t dir_size_bytes(const std::string& path) {
  struct stat st{};
  if (::lstat(path.c_str(), &st) != 0) return 0;

  if (S_ISDIR(st.st_mode)) {
    uint64_t sum = 0;
    DIR* d = ::opendir(path.c_str());
    if (!d) return 0;
    struct dirent* ent = nullptr;
    while ((ent = ::readdir(d)) != nullptr) {
      if (!std::strcmp(ent->d_name, ".") || !std::strcmp(ent->d_name, "..")) continue;
      std::string child = path + "/" + ent->d_name;
      sum += dir_size_bytes(child);
    }
    ::closedir(d);
    return sum;
  }

  return (uint64_t)st.st_size;
}

struct EventDirInfo {
  uint32_t event_id = 0;
  int64_t  mtime_s  = 0;
  std::string path;
};

/**
 * @brief 枚举 `list_event_dirs_oldest_first` 对应集合。
 * @param out 输出对象。
 */
inline void list_event_dirs_oldest_first(std::vector<EventDirInfo>& out) {
  out.clear();
  DIR* d = ::opendir(kEventsDir);
  if (!d) return;

  struct dirent* ent = nullptr;
  while ((ent = ::readdir(d)) != nullptr) {
    if (!is_hex8_dirname(ent->d_name)) continue;
    const uint32_t eid = parse_hex8_u32(ent->d_name);
    std::string dir = std::string(kEventsDir) + "/" + ent->d_name;

    struct stat st{};
    if (::stat(dir.c_str(), &st) != 0) continue;

    EventDirInfo it;
    it.event_id = eid;
    it.mtime_s = (int64_t)st.st_mtime;
    it.path = std::move(dir);
    out.push_back(std::move(it));
  }
  ::closedir(d);

  std::sort(out.begin(), out.end(), [](const EventDirInfo& a, const EventDirInfo& b){
    if (a.mtime_s != b.mtime_s) return a.mtime_s < b.mtime_s;  // 最旧在前
    return a.event_id < b.event_id;
  });
}

//inline 内联函数：将函数体插入到调用处来优化性能的机制，它以空间换时间，适合短小、高频调用的函数，主要特点是建议编译器展开函数调用以减少开销。
/**
 * @brief 确保 `ensure_dir` 对应条件成立。
 * @param path 路径。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool ensure_dir(const std::string& path) {// 递归创建目录
  if (path.empty()) return false;// 空路径错误
  if (path == "/") return true;// 根目录默认存在
  std::string cur;// 当前构建的路径
  cur.reserve(path.size());//初始化容量
  for (size_t i = 0; i < path.size(); ++i) {// 遍历路径字符
    const char c = path[i];// 当前字符
    cur.push_back(c);// 添加到当前路径
    if (c == '/' || i + 1 == path.size()) {// 遇到路径分隔符或结尾
      if (cur.size() == 1) continue; // "/"
      if (::mkdir(cur.c_str(), 0755) != 0) {// mkdir是Linux系统调用，创建目录 0755是对应权限
        if (errno != EEXIST) return false;//errno是系统错误码，EEXIST表示目录已存在
      }
    }
  }
  return true;
}

/**
 * @brief 构建 `build_bcdtime8_array` 对应结果。
 * @param out8 8 字节输出缓冲区。
 */
inline void build_bcdtime8_array(uint8_t out8[8]) {// 获取当前系统时间并转换为 BCD 格式存入 out8 数组
  std::vector<uint8_t> t8;
  btt::proto::build_bcdtime8(t8);
  if (t8.size() >= 8) std::memcpy(out8, t8.data(), 8);
  else std::memset(out8, 0, 8);
}

/**
 * @brief 向输出缓冲区追加 `append_u32_be` 对应内容。
 * @param out 输出对象。
 * @param v 输入数值。
 */
inline void append_u32_be(std::vector<uint8_t>& out, uint32_t v) {
  out.push_back(uint8_t((v >> 24) & 0xFF));
  out.push_back(uint8_t((v >> 16) & 0xFF));
  out.push_back(uint8_t((v >> 8) & 0xFF));
  out.push_back(uint8_t(v & 0xFF));
}

class RealTimeStreamer {
public:
  /**
   * @brief 构造 `RealTimeStreamer` 对象并初始化默认状态。
   */
  RealTimeStreamer() = default;
  /**
   * @brief 析构 `RealTimeStreamer` 对象并释放关联资源。
   */
  ~RealTimeStreamer() { stop_and_join(false, 0); }

  /**
   * @brief 执行 `attach` 内部辅助逻辑。
   * @param rt 共享运行态。
   * @param ctrl_out 控制输出队列。
   */
  void attach(RuntimeConfig& rt, btt::utils::BlockingQueue<btt::proto::Frame>& ctrl_out) {
    rt_ = &rt;
    ctrl_out_ = &ctrl_out;
  }

  /**
   * @brief 查询当前对象是否处于活动状态。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool active() const { return active_.load(); }
  /**
   * @brief 获取当前活动摄像头编号。
   * @return 返回计算结果。
   */
  uint8_t cam_id() const { return cam_id_.load(); }

  /**
   * @brief 执行 `match_params` 内部辅助逻辑。
   * @param cam_id 摄像头 ID。
   * @param ip_be 大端 IPv4 地址。
   * @param port 端口号。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool match_params(uint8_t cam_id, uint32_t ip_be, uint16_t port) const {
    return active_.load() && cam_id_.load() == cam_id && ip_be_.load() == ip_be && port_.load() == port;
  }

  /**
   * @brief 执行 `start` 内部辅助逻辑。
   * @param cam_id 摄像头 ID。
   * @param ip_be 大端 IPv4 地址。
   * @param port 端口号。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool start(uint8_t cam_id, uint32_t ip_be, uint16_t port) {
    if (active_.load()) return false;

    stop_and_join(false, 0);

    const std::string ip = ip_be_to_string(ip_be);
    if (!media_.connect_to(ip, port, 3000)) {
      return false;
    }

    {
      std::lock_guard<std::mutex> lk(mu_);
      q_.clear();
      drop_count_ = 0;
    }

    cam_id_.store(cam_id);
    ip_be_.store(ip_be);
    port_.store(port);
    stream_seq_.store(0);
    btt_seq_.store(1);
    last_keepalive_ms_.store(now_ms_steady());
    end_notified_.store(false);

    stop_.store(false);
    active_.store(true);
    th_ = std::thread([this]{ sender_loop(); });
    return true;
  }

  /**
   * @brief 刷新保活时间戳。
   */
  void keepalive() {
    last_keepalive_ms_.store(now_ms_steady());
  }

  /**
   * @brief 关闭 `close_by_host` 对应对象。
   * @param cam_id 摄像头 ID。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool close_by_host(uint8_t cam_id) {
    if (!active_.load()) return false;
    if (cam_id_.load() != cam_id) return false;
    stop_and_join(false, 0);
    return true;
  }

  /**
   * @brief 执行 `tick_keepalive` 内部辅助逻辑。
   */
  void tick_keepalive() {
    if (!active_.load()) return;
    const int64_t last = last_keepalive_ms_.load();
    if (last <= 0) return;
    const int64_t now = now_ms_steady();
    if (now - last >= kKeepaliveTimeoutMs) {
      LOGW("RT_STREAM keepalive timeout: cam=%u", cam_id_.load());
      stop_and_join(true, 1);
    }
  }

  /**
   * @brief 执行 `push_frame` 内部辅助逻辑。
   * @param ch 通道号。
   * @param fi 媒体帧信息。
   * @param buf 数据缓冲区。
   * @param len 数据长度。
   */
  void push_frame(int ch, FRAME_INFO fi, const char* buf, int len) {
    if (!active_.load()) return;
    if (ch != cam_id_.load()) return;
    if (!buf || len <= 0) return;

    StreamFrame f;
    f.is_i = (fi.frame_type == FRAME_I);
    f.data.assign(reinterpret_cast<const uint8_t*>(buf),
                  reinterpret_cast<const uint8_t*>(buf) + len);

    {
      std::lock_guard<std::mutex> lk(mu_);
      if (q_.size() >= kQueueMaxFrames) {
        q_.pop_front();
        drop_count_++;
        if ((drop_count_ % 100) == 1) {
          LOGW("RT_STREAM drop frame: cam=%u dropped=%zu", cam_id_.load(), drop_count_);
        }
      }
      q_.push_back(std::move(f));
    }
    cv_.notify_one();
  }

  /**
   * @brief 停止 `stop_and_join` 对应流程。
   * @param notify 是否发送通知。
   * @param reason 原因码。
   */
  void stop_and_join(bool notify, uint8_t reason) {
    const bool join = true;
    stop_internal(notify, reason, join);
  }

private:
  struct StreamFrame {
    std::vector<uint8_t> data;
    bool is_i = false;
  };

  /**
   * @brief 向输出缓冲区追加 `append_u16_be_local` 对应内容。
   * @param out 输出对象。
   * @param v 输入数值。
   */
  static void append_u16_be_local(std::vector<uint8_t>& out, uint16_t v) {
    out.push_back(uint8_t((v >> 8) & 0xFF));
    out.push_back(uint8_t(v & 0xFF));
  }

  /**
   * @brief 发送 `notify_end` 对应通知。
   * @param reason 原因码。
   */
  void notify_end(uint8_t reason) {
    if (!rt_ || !ctrl_out_) return;
    bool expected = false;
    if (!end_notified_.compare_exchange_strong(expected, true)) return;

    btt::proto::Frame f;
    f.cmd = 0x37;
    f.level = 0x00;
    f.seq = rt_->seq.fetch_add(1);
    f.payload.push_back(cam_id_.load());
    f.payload.push_back(reason);
    ctrl_out_->push(std::move(f));
  }

  /**
   * @brief 停止 `stop_internal` 对应流程。
   * @param notify 是否发送通知。
   * @param reason 原因码。
   * @param join 是否等待线程退出。
   */
  void stop_internal(bool notify, uint8_t reason, bool join) {
    active_.store(false);
    stop_.store(true);
    media_.close_fd();
    {
      std::lock_guard<std::mutex> lk(mu_);
      q_.clear();
    }
    cv_.notify_all();
    if (notify) notify_end(reason);
    if (join) {
      if (th_.joinable() && std::this_thread::get_id() != th_.get_id()) {
        th_.join();
      }
      stop_.store(false);
    }
  }

  /**
   * @brief 执行 `sender_loop` 内部辅助逻辑。
   */
  void sender_loop() {
    while (true) {
      StreamFrame fr;
      {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&]{ return stop_.load() || !q_.empty(); });
        if (stop_.load() && q_.empty()) break;
        if (q_.empty()) continue;
        fr = std::move(q_.front());
        q_.pop_front();
      }

      if (!active_.load() || fr.data.empty()) continue;

      size_t off = 0;
      const bool is_i = fr.is_i;
      while (off < fr.data.size()) {
        if (stop_.load() || !active_.load()) break;
        const size_t chunk = std::min(kChunkMax, fr.data.size() - off);

        uint8_t flags = 0x00;
        if (off + chunk >= fr.data.size()) flags |= 0x01; // D0 boundary
        if (is_i) flags |= 0x02; // D1-D3 frame type = 1 (I)

        btt::proto::Frame f;
        f.cmd = 0x51;
        f.level = 0x00;
        f.seq = btt_seq_.fetch_add(1);

        f.payload.reserve(4 + chunk);
        append_u16_be_local(f.payload, stream_seq_.fetch_add(1));
        f.payload.push_back(0x00); // type: video
        f.payload.push_back(flags);
        f.payload.insert(f.payload.end(), fr.data.begin() + off, fr.data.begin() + off + chunk);

        auto bytes = btt::proto::encode(f);
        if (bytes.empty() || !media_.send_all(bytes.data(), bytes.size())) {
          LOGW("RT_STREAM send failed, cam=%u", cam_id_.load());
          stop_internal(true, 2, false);
          return;
        }

        off += chunk;
      }
    }
  }

  static constexpr size_t kChunkMax = 1024;
  static constexpr size_t kQueueMaxFrames = 32;
  static constexpr int64_t kKeepaliveTimeoutMs = 60 * 1000;

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<StreamFrame> q_;
  size_t drop_count_ = 0;

  btt::net::TcpClient media_;
  std::thread th_;

  std::atomic<bool> active_{false};
  std::atomic<bool> stop_{false};
  std::atomic<bool> end_notified_{false};

  std::atomic<uint8_t> cam_id_{0};
  std::atomic<uint32_t> ip_be_{0};
  std::atomic<uint16_t> port_{0};

  std::atomic<uint16_t> stream_seq_{0};
  std::atomic<uint16_t> btt_seq_{1};
  std::atomic<int64_t> last_keepalive_ms_{0};

  RuntimeConfig* rt_ = nullptr;
  btt::utils::BlockingQueue<btt::proto::Frame>* ctrl_out_ = nullptr;
};


/**
 * @brief 向输出缓冲区追加 `append_fileinfo25` 对应内容。
 * @param out 输出对象。
 * @param f `f` 参数。
 */
inline void append_fileinfo25(std::vector<uint8_t>& out, const FileInfo25& f) {//M3 文件信息结构体打包到字节流
  append_u32_be(out, f.file_id);//文件唯一标识ID。
  append_u32_be(out, f.file_len);//文件长度
  out.insert(out.end(), f.start_bcd, f.start_bcd + 8);//开始时间
  out.push_back(f.flag);//标志
  append_u32_be(out, f.duration_ms);//持续时间（毫秒）
  out.push_back(f.pos_mask);
  out.push_back(uint8_t((f.reserved >> 8) & 0xFF));
  out.push_back(uint8_t(f.reserved & 0xFF));
  out.push_back(f.file_type);
}

/**
 * @brief 执行 `cam_pos_mask4_from_cfg` 内部辅助逻辑。
 * @param rt 共享运行态。
 * @param cam_id 摄像头 ID。
 * @return 返回计算结果。
 */
inline uint8_t cam_pos_mask4_from_cfg(RuntimeConfig& rt, uint8_t cam_id) {// 按摄像头ID写入位置字段（0..3）
  (void)rt;
  if (cam_id > 3) return 0;
  return uint8_t(cam_id & 0x0F);
}

class MediaManager {
public:
  /**
   * @brief 构造 `MediaManager` 对象并初始化默认状态。
   */
  MediaManager() {//构造函数，初始化成员变量
    // 用时间做种子，避免重启后 file_id 从 1 开始导致重复
    const uint32_t seed = uint32_t(::time(nullptr));//获取当前时间作为种子，重点nullprt 比null更安全,尽量使用nullprt
    //seed ^ 常数作为 搅乱初始值，让它随机一下
    next_event_id_.store((seed ^ 0xA55A5AA5u) & kIdBodyMask);//事件ID主体
    next_file_id_.store(((seed << 16) ^ 0x5AA55AA5u) & kIdBodyMask);//文件ID主体
    for (auto& r : rec_) { r.fd = -1; }//便利初始化录像通道文件描述符为-1，表示未打开，c++11 最新用法 for((auto &（公式） it （遍历的成员对象）: rec_(遍历的对象))
    self_ptr() = this;//静态指针指向当前对象实例
  }

  /**
   * @brief 执行 `init_once` 内部辅助逻辑。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool init_once() {// 媒体库初始化（仅执行一次）
    std::lock_guard<std::mutex> lk(mu_);//mu_ 互斥锁，保护初始化过程线程安全,当前类私有成员
    if (inited_) return true;// 已初始化直接返回成功

    int rc = HiF_media_init();
    if (rc != 0) {
      LOGE("HiF_media_init failed rc=%d", rc);
      return false;
    }

    for (int ch = 0; ch < 4; ++ch) {
      hif_media_param_t p{};
      (void)HiF_media_get_param(ch, &p);
      p.pic_size   = HIF_PIC_D1_NTSC;
      p.payload    = VE_TYPE_H264;
      p.frame_rate = 25;
      p.rc_mode    = VENC_RC_CBR;
      p.bit_rate   = 550;
      p.max_bit_rate = 2 * 1024;
      p.long_term_min_bit_rate = 1 * 1024;
      p.cb_get_stream = (ch == 0) ? &MediaManager::stream_ch0 :
                        (ch == 1) ? &MediaManager::stream_ch1 :
                        (ch == 2) ? &MediaManager::stream_ch2 :
                                   &MediaManager::stream_ch3;
      rc = HiF_media_set_param(ch, &p);
      if (rc != 0) {
        LOGE("HiF_media_set_param ch=%d failed rc=%d", ch, rc);
        return false;
      }
    }

    rc = HiF_media_start_encode();
    if (rc != 0) {
      LOGE("HiF_media_start_encode failed rc=%d", rc);
      return false;
    }

    inited_ = true;
    LOGI("HiF media inited + encoding started");
    return true;
  }

  /**
   * @brief 设置 `set_streamer` 对应状态。
   * @param s 输入对象。
   */
  void set_streamer(RealTimeStreamer* s) { streamer_ = s; }
  /**
   * @brief 设置 `set_video_max_ms` 对应状态。
   * @param kVideoMaxMs `kVideoMaxMs` 参数。
   */
  void set_video_max_ms(uint32_t ms) { video_max_ms_.store(ms ? ms : kVideoMaxMs); }

  // 当前录制事件（用于 0x3A 停止后写 meta / 0x3E 查询）
  /**
   * @brief 执行 `current_event` 内部辅助逻辑。
   * @param event_id 事件 ID。
   * @param dir 目录路径。
   * @param light_fill 补光标志。
   * @param laser 激光标志。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool current_event(uint32_t& event_id, std::string& dir, uint8_t& light_fill, uint8_t& laser) const {
    // std::lock_guard<std::mutex> lk(mu_);
    event_id = cur_event_id_;
    dir = cur_event_dir_;
    light_fill = cur_light_fill_;
    laser = cur_laser_;
    return (event_id != 0 && !dir.empty());
  }

  /**
   * @brief 清理 `clear_current_event` 相关状态。
   */
  void clear_current_event() {
    std::lock_guard<std::mutex> lk(mu_);
    cur_event_id_ = 0;
    cur_event_dir_.clear();
    cur_light_fill_ = 0;
    cur_laser_ = 0;
    cur_files_.clear();
    for (auto& r : rec_) {
      r.active = false;
      r.wait_i = true;
      r.finalized = false;
      if (r.fd >= 0) { ::close(r.fd); r.fd = -1; }
      r.file_id = 0;
      r.bytes = 0;
      r.start_ms_steady = 0;
      std::memset(r.start_bcd, 0, 8);
      r.pos_mask4 = 0;
      r.cam_id = 0;
      std::memset(r.path, 0, sizeof(r.path));
    }
  }

  // 生成事件目录：/data/events/<8HEX>/ （带容量准入）
/**
 * @brief 执行 `create_event_dir` 内部辅助逻辑。
 * @param need_bytes 所需空间大小。
 * @param event_id 事件 ID。
 * @param dir 目录路径。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
bool create_event_dir(uint64_t need_bytes, uint32_t& event_id, std::string& dir) {
  // 先保证根目录存在
  if (!ensure_dir(kEventsDir)) {
    LOGE("ensure_dir(%s) failed errno=%d", kEventsDir, errno);
    return false;
  }

  // 准入控制：必须确保“肯定够上界”；不够则删旧事件直到够
  if (!ensure_capacity(need_bytes)) {
    LOGE("ensure_capacity failed need=%llu (reserve=%llu, budget=%llu)",
         (unsigned long long)need_bytes,
         (unsigned long long)kReserveBytes,
         (unsigned long long)kEventBudgetBytes);
    return false;
  }

  event_id = alloc_tagged_id(next_event_id_, kEventIdTag);
  char buf[128];
  std::snprintf(buf, sizeof(buf), "%s/%08X", kEventsDir, event_id);
  dir = buf;

  if (!ensure_dir(dir)) {
    LOGE("ensure_dir(%s) failed errno=%d", dir.c_str(), errno);
    return false;
  }
  return true;
}

  // 0x30：抓拍并保存 jpg
  /**
   * @brief 执行 `snap_jpg_to_event` 内部辅助逻辑。
   * @param cam_id 摄像头 ID。
   * @param light_fill 补光标志。
   * @param laser 激光标志。
   * @param pos_mask4 `pos_mask4` 参数。
   * @param snap_type `snap_type` 参数。
   * @param need_bytes 所需空间大小。
   * @param event_id 事件 ID。
   * @param dir 目录路径。
   * @param out 输出对象。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool snap_jpg_to_event(uint8_t cam_id, uint8_t light_fill, uint8_t laser,
                         uint8_t pos_mask4, uint8_t snap_type,
                         uint64_t need_bytes,
                         uint32_t& event_id, std::string& dir, FileInfo25& out) {
    (void)snap_type; // 当前阶段仅透传回包

    if (!init_once()) return false;
    if (!create_event_dir(need_bytes, event_id, dir)) return false;

    std::vector<uint8_t> jpg;
    jpg.resize(1024 * 1024);
    int len = HiF_media_snap(cam_id, jpg.data(), (int)jpg.size());
    if (len <= 0) {
      LOGE("HiF_media_snap ch=%u failed len=%d", cam_id, len);
      return false;
    }
    jpg.resize((size_t)len);

    const uint32_t file_id = alloc_tagged_id(next_file_id_, kFileIdTag);
    const std::string path = make_media_file_path_for_write(dir, file_id, "jpg");

    int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) {
      LOGE("open(%s) failed errno=%d", path.c_str(), errno);
      return false;
    }
    ssize_t w = ::write(fd, jpg.data(), jpg.size());
    ::close(fd);
    if (w != (ssize_t)jpg.size()) {
      LOGE("write(%s) short w=%zd expect=%zu errno=%d", path.c_str(), w, jpg.size(), errno);
      return false;
    }

    build_bcdtime8_array(out.start_bcd);
    out.file_id = file_id;
    out.file_len = (uint32_t)jpg.size();
    out.flag = 0xFF;
    out.duration_ms = 0;
    out.pos_mask = pos_mask4;
    out.reserved = 0;
    out.file_type = uint8_t(((laser & 0x01) << 7) | ((light_fill & 0x01) << 6) | ((cam_id & 0x03) << 4) | 0x00);

    LOGI("SNAP OK: cam=%u fill=%u laser=%u path=%s len=%u pos_mask=0x%02X file_type=0x%02X",
         cam_id, light_fill, laser, path.c_str(), out.file_len, out.pos_mask, out.file_type);
    return true;
  }


  // M5: 应用视频参数（分辨率/帧率/码率）到 4 路编码通道
  /**
   * @brief 应用 `apply_video_params_from_rt` 对应配置。
   * @param rt 共享运行态。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool apply_video_params_from_rt(const RuntimeConfig& rt) {
    int HiF_media_exit();

    int rc = HiF_media_init();
    if (rc != 0) {
      LOGE("HiF_media_init failed rc=%d", rc);
      return false;
    }
    hif_pic_size pic_size ;
    if (!map_res_code_to_hif(rt.m5_video_res.load(), pic_size)) return false;

    const uint32_t kbps = uint32_t(rt.video_bitrate_kbps.load());
    const uint8_t fps = rt.m5_video_fps.load();

    for (int ch = 0; ch < 4; ++ch) {
      hif_media_param_t p{};
      (void)HiF_media_get_param(ch, &p);
      p.pic_size   = pic_size;
      p.payload    = VE_TYPE_H264;
      p.frame_rate = fps;
      p.rc_mode    = VENC_RC_CBR;
      p.bit_rate   = int(kbps);
      p.max_bit_rate = 2 * 1024;
      p.long_term_min_bit_rate = 1 * 1024;
      p.cb_get_stream = (ch == 0) ? &MediaManager::stream_ch0 :
                        (ch == 1) ? &MediaManager::stream_ch1 :
                        (ch == 2) ? &MediaManager::stream_ch2 :
                                   &MediaManager::stream_ch3;
      int rc = HiF_media_set_param(ch, &p);
      if (rc != 0) {
        LOGE("HiF_media_set_param (M5 video) ch=%d failed rc=%d", ch, rc);
        return false;
      }

    }
    // HiF_media_start_encode();
    LOGI("M5 video params applied: res_code=%u fps=%u kbps=%u", rt.m5_video_res.load(), fps, kbps);
    return true;
  }

  // M5: 4 路抓拍（同一 event_id / 同一目录），用于 0x38 或周期测量
  /**
   * @brief 执行 `measure_snap4_to_event` 内部辅助逻辑。
   * @param rt 共享运行态。
   * @param light_fill 补光标志。
   * @param laser 激光标志。
   * @param need_bytes 所需空间大小。
   * @param event_id 事件 ID。
   * @param dir 目录路径。
   * @param files_out 输出参数。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool measure_snap4_to_event(const RuntimeConfig& rt,
                             uint8_t light_fill, uint8_t laser,
                             uint64_t need_bytes,
                             uint32_t& event_id, std::string& dir,
                             std::vector<FileInfo25>& files_out) {
    files_out.clear();
    if (!init_once()) return false;

    // 抓拍分辨率：优先使用 m5_pic_res
    hif_pic_size pic_size ;
    if (!map_res_code_to_hif(rt.m5_pic_res.load(), pic_size)) return false;
    for (int ch = 0; ch < 4; ++ch) {
      hif_media_param_t p{};
      (void)HiF_media_get_param(ch, &p);
      p.pic_size = pic_size;
      int rc = HiF_media_set_param(ch, &p);
      if (rc != 0) {
        LOGE("HiF_media_set_param (M5 pic) ch=%d failed rc=%d", ch, rc);
        return false;
      }
    }

    if (!create_event_dir(need_bytes, event_id, dir)) return false;

    files_out.reserve(4);
    for (uint8_t cam_id = 0; cam_id < 4; ++cam_id) {
      std::vector<uint8_t> jpg;
      jpg.resize(1024 * 1024);
      int len = HiF_media_snap(cam_id, jpg.data(), (int)jpg.size());
      if (len <= 0) {
        LOGE("HiF_media_snap ch=%u failed len=%d", cam_id, len);
        return false;
      }
      jpg.resize((size_t)len);

      const uint32_t file_id = alloc_tagged_id(next_file_id_, kFileIdTag);
      const std::string path = make_media_file_path_for_write(dir, file_id, "jpg");

      int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
      if (fd < 0) {
        LOGE("open(%s) failed errno=%d", path.c_str(), errno);
        return false;
      }
      ssize_t w = ::write(fd, jpg.data(), jpg.size());
      ::close(fd);
      if (w != (ssize_t)jpg.size()) {
        LOGE("write(%s) short w=%zd expect=%zu errno=%d", path.c_str(), w, jpg.size(), errno);
        return false;
      }

      FileInfo25 f;
      build_bcdtime8_array(f.start_bcd);
      f.file_id = file_id;
      f.file_len = (uint32_t)jpg.size();
      f.flag = 0xFF;
      f.duration_ms = 0;
      f.pos_mask = cam_pos_mask4_from_cfg(const_cast<RuntimeConfig&>(rt), cam_id);
      f.reserved = 0;
      f.file_type = uint8_t(((laser & 0x01) << 7) | ((light_fill & 0x01) << 6) | ((cam_id & 0x03) << 4) | 0x00);
      files_out.push_back(f);

      LOGI("MEAS SNAP OK: cam=%u path=%s len=%u pos_mask=0x%02X", cam_id, path.c_str(), f.file_len, f.pos_mask);
    }
    return true;
  }

  // M8: 在既有事件目录下补拍 4 路 JPG（用于操岔/过车结束阶段）
  /**
   * @brief 执行 `snap4_jpg_to_dir` 内部辅助逻辑。
   * @param rt 共享运行态。
   * @param light_fill 补光标志。
   * @param laser 激光标志。
   * @param dir 目录路径。
   * @param files_out 输出参数。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool snap4_jpg_to_dir(const RuntimeConfig& rt,
                        uint8_t light_fill, uint8_t laser,
                        const std::string& dir,
                        std::vector<FileInfo25>& files_out) {
    files_out.clear();
    if (!init_once()) return false;

    // 抓拍分辨率：优先使用 m5_pic_res
    hif_pic_size pic_size;
    if (!map_res_code_to_hif(rt.m5_pic_res.load(), pic_size)) return false;
    for (int ch = 0; ch < 4; ++ch) {
      hif_media_param_t p{};
      (void)HiF_media_get_param(ch, &p);
      p.pic_size = pic_size;
      int rc = HiF_media_set_param(ch, &p);
      if (rc != 0) {
        LOGE("HiF_media_set_param (M8 pic) ch=%d failed rc=%d", ch, rc);
        return false;
      }
    }

    files_out.reserve(4);
    for (uint8_t cam_id = 0; cam_id < 4; ++cam_id) {
      std::vector<uint8_t> jpg;
      jpg.resize(1024 * 1024);
      int len = HiF_media_snap(cam_id, jpg.data(), (int)jpg.size());
      if (len <= 0) {
        LOGE("HiF_media_snap (M8) ch=%u failed len=%d", cam_id, len);
        return false;
      }
      jpg.resize((size_t)len);

      const uint32_t file_id = alloc_tagged_id(next_file_id_, kFileIdTag);
      const std::string path = make_media_file_path_for_write(dir, file_id, "jpg");

      int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
      if (fd < 0) {
        LOGE("open(%s) failed errno=%d", path.c_str(), errno);
        return false;
      }
      ssize_t w = ::write(fd, jpg.data(), jpg.size());
      ::close(fd);
      if (w != (ssize_t)jpg.size()) {
        LOGE("write(%s) short w=%zd expect=%zu errno=%d", path.c_str(), w, jpg.size(), errno);
        return false;
      }

      FileInfo25 f;
      build_bcdtime8_array(f.start_bcd);
      f.file_id = file_id;
      f.file_len = (uint32_t)jpg.size();
      f.flag = 0xFF;
      f.duration_ms = 0;
      f.pos_mask = cam_pos_mask4_from_cfg(const_cast<RuntimeConfig&>(rt), cam_id);
      f.reserved = 0;
      f.file_type = uint8_t(((laser & 0x01) << 7) | ((light_fill & 0x01) << 6) | ((cam_id & 0x03) << 4) | 0x00);
      files_out.push_back(f);

      LOGI("M8 SNAP OK: cam=%u path=%s len=%u pos_mask=0x%02X", cam_id, path.c_str(), f.file_len, f.pos_mask);
    }
    return true;
  }

  // 0x39：开始录像（保存 h264 裸流）
  /**
   * @brief 启动 `start_record` 对应流程。
   * @param cam_sel 摄像头选择值。
   * @param all 是否作用于全部通道。
   * @param light_fill 补光标志。
   * @param laser 激光标志。
   * @param rt 共享运行态。
   * @param need_bytes 所需空间大小。
   * @param event_id 事件 ID。
   * @param dir 目录路径。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool start_record(uint8_t cam_sel, bool all, uint8_t light_fill, uint8_t laser,
                    RuntimeConfig& rt,
                    uint64_t need_bytes,
                    uint32_t& event_id, std::string& dir) {
    if (!init_once()) return false;
    if (!create_event_dir(need_bytes, event_id, dir)) return false;

    std::lock_guard<std::mutex> lk(mu_);
    cur_event_id_ = event_id;
    cur_event_dir_ = dir;
    cur_light_fill_ = light_fill;
    cur_laser_ = laser;
    cur_files_.clear();
    for (auto& r : rec_) { r.finalized = false; }

    auto open_one = [&](uint8_t ch) -> bool {
      RecCtx& r = rec_[ch];
      if (r.fd >= 0) { ::close(r.fd); r.fd = -1; }

      const uint32_t file_id = alloc_tagged_id(next_file_id_, kFileIdTag);
      const std::string path = make_media_file_path_for_write(dir, file_id, "h264");
      int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
      if (fd < 0) {
        LOGE("open(%s) failed errno=%d", path.c_str(), errno);
        return false;
      }
      r.active = true;
      r.wait_i = true;
      r.fd = fd;
      r.file_id = file_id;
      r.bytes = 0;
      r.start_ms_steady = now_ms_steady();;
      build_bcdtime8_array(r.start_bcd);
      r.pos_mask4 = cam_pos_mask4_from_cfg(rt, ch);
      r.cam_id = ch;
      std::snprintf(r.path, sizeof(r.path), "%s", path.c_str());

      LOGI("REC START: ch=%u path=%s pos_mask=0x%02X", ch, path.c_str(), r.pos_mask4);
      return true;
    };

    if (all) {
      for (uint8_t ch = 0; ch < 4; ++ch) {
        if (!open_one(ch)) return false;
      }
      return true;
    }

    if (cam_sel > 3) return false;
    return open_one(cam_sel);
  }

  // 0x3A：停止录像，返回文件信息列表（当前实现：只返回本次停止涉及的文件）
  // 0x3A：停止录像，返回文件信息列表（支持 15s 自动停录后再 stop）
/**
 * @brief 停止 `stop_record` 对应流程。
 * @param cam_sel 摄像头选择值。
 * @param all 是否作用于全部通道。
 * @param light_fill 补光标志。
 * @param laser 激光标志。
 * @param files_out 输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
bool stop_record(uint8_t cam_sel, bool all, uint8_t light_fill, uint8_t laser,
                 std::vector<FileInfo25>& files_out) {
  std::lock_guard<std::mutex> lk(mu_);
  (void)light_fill; (void)laser; // 文件类型位使用 start_record 时的状态

  files_out.clear();

  auto finalize_one = [&](uint8_t ch, const char* why) {
    RecCtx& r = rec_[ch];
    if (r.finalized) return;

    // 还在录：先停
    if (r.active) {
      r.active = false;
      if (r.fd >= 0) { ::close(r.fd); r.fd = -1; }
    } else {
      // 已经因为 15s 超时停掉：fd 可能已关闭
      if (r.fd >= 0) { ::close(r.fd); r.fd = -1; }
    }

    // 没有产生有效数据（比如始终等 I 帧）就不记入 files
    if (r.file_id == 0) return;

    FileInfo25 f;
    f.file_id = r.file_id;
    f.file_len = (uint32_t)r.bytes;
    std::memcpy(f.start_bcd, r.start_bcd, 8);
    f.flag = 0xFF;
    const int64_t dur = now_ms_steady() - r.start_ms_steady;
    f.duration_ms = (dur > 0) ? (uint32_t)dur : 0;
    f.pos_mask = r.pos_mask4;
    f.reserved = 0;
    f.file_type = uint8_t(((cur_laser_ & 0x01) << 7) | ((cur_light_fill_ & 0x01) << 6) | ((r.cam_id & 0x03) << 4) | 0x01);

    cur_files_.push_back(f);
    r.finalized = true;

    LOGI("REC FINALIZE(%s): ch=%u file_id=%08X bytes=%llu dur_ms=%u file_type=0x%02X",
         why, ch, f.file_id, (unsigned long long)r.bytes, f.duration_ms, f.file_type);
  };

  if (all) {
    for (uint8_t ch = 0; ch < 4; ++ch) finalize_one(ch, "STOP_ALL");
  } else {
    if (cam_sel > 3) return false;
    finalize_one(cam_sel, "STOP_ONE");
  }

  files_out = cur_files_;
  return !files_out.empty();
}

/**
 * @brief 确保 `ensure_capacity` 对应条件成立。
 * @param need_bytes 所需空间大小。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
bool ensure_capacity(uint64_t need_bytes) {
  // 两道闸：
  // 1) 文件系统余量：free(/data) >= need + reserve
  // 2) 事件预算：used(/data/events) + need <= event_budget
  // 不关心是否上报过；永远按最旧事件删除。

  std::vector<EventDirInfo> dirs;
  list_event_dirs_oldest_first(dirs);
  size_t del_idx = 0;

  for (int iter = 0; iter < 512; ++iter) {
    const uint64_t free_b = fs_free_bytes("/data");
    const uint64_t used_b = dir_size_bytes(kEventsDir);

    const bool ok_free   = (free_b >= need_bytes + kReserveBytes);
    const bool ok_budget = (used_b + need_bytes <= kEventBudgetBytes);

    if (ok_free && ok_budget) return true;

    if (del_idx >= dirs.size()) {
      LOGW("ensure_capacity: not enough space (free=%llu used=%llu need=%llu), and no more events to delete",
           (unsigned long long)free_b, (unsigned long long)used_b, (unsigned long long)need_bytes);
      return false;
    }

    const auto& victim = dirs[del_idx++];
    LOGW("ensure_capacity: delete oldest event %08X (mtime=%lld) path=%s",
         victim.event_id, (long long)victim.mtime_s, victim.path.c_str());
    (void)rm_rf(victim.path);
    // 继续循环，重新评估 free/used
  }

  LOGW("ensure_capacity: too many iterations, abort");
  return false;
}
private:
  struct RecCtx {
    bool active = false;
    bool wait_i = true;
    bool finalized = false;
    int  fd = -1;
    uint32_t file_id = 0;
    uint64_t bytes = 0;
    int64_t start_ms_steady = 0;
    uint8_t start_bcd[8]{};
    uint8_t pos_mask4 = 0;
    uint8_t cam_id = 0;
    char path[256]{};
  };

  /**
   * @brief 执行 `stream_ch0` 内部辅助逻辑。
   * @param fi 媒体帧信息。
   * @param buf 数据缓冲区。
   * @param s 输入对象。
   * @return 返回计算结果。
   */
  static int stream_ch0(FRAME_INFO fi, char* buf, int len) { auto* s = self_ptr(); return s ? s->on_stream(0, fi, buf, len) : 0; }
  /**
   * @brief 执行 `stream_ch1` 内部辅助逻辑。
   * @param fi 媒体帧信息。
   * @param buf 数据缓冲区。
   * @param s 输入对象。
   * @return 返回计算结果。
   */
  static int stream_ch1(FRAME_INFO fi, char* buf, int len) { auto* s = self_ptr(); return s ? s->on_stream(1, fi, buf, len) : 0; }
  /**
   * @brief 执行 `stream_ch2` 内部辅助逻辑。
   * @param fi 媒体帧信息。
   * @param buf 数据缓冲区。
   * @param s 输入对象。
   * @return 返回计算结果。
   */
  static int stream_ch2(FRAME_INFO fi, char* buf, int len) { auto* s = self_ptr(); return s ? s->on_stream(2, fi, buf, len) : 0; }
  /**
   * @brief 执行 `stream_ch3` 内部辅助逻辑。
   * @param fi 媒体帧信息。
   * @param buf 数据缓冲区。
   * @param s 输入对象。
   * @return 返回计算结果。
   */
  static int stream_ch3(FRAME_INFO fi, char* buf, int len) { auto* s = self_ptr(); return s ? s->on_stream(3, fi, buf, len) : 0; }

  /**
   * @brief 执行 `on_stream` 内部辅助逻辑。
   * @param ch 通道号。
   * @param fi 媒体帧信息。
   * @param buf 数据缓冲区。
   * @param len 数据长度。
   * @return 返回计算结果。
   */
  int on_stream(int ch, FRAME_INFO fi, char* buf, int len) {
  if (!buf || len <= 0) return 0;
  if (ch < 0 || ch >= 4) return 0;

  if (streamer_) {
    streamer_->push_frame(ch, fi, buf, len);
  }

  std::lock_guard<std::mutex> lk(mu_); // 保护录像状态
  RecCtx& r = rec_[ch];

  const uint32_t vmax = video_max_ms_.load();

  // 等 I 帧阶段也要受最大时长保护，避免卡死
  const int64_t now = now_ms_steady();
  if (r.active && r.wait_i && (now - r.start_ms_steady >= (int64_t)vmax)) {
    // 直接关掉（不写入 files，后续 stop 时也会返回空）
    r.active = false;
    if (r.fd >= 0) { ::close(r.fd); r.fd = -1; }
    r.finalized = true;
    LOGW("REC ch=%d timeout while waiting I-frame, force stop (vmax=%ums)", ch, vmax);
    return 0;
  }

  if (!r.active || r.fd < 0) return 0;

  if (r.wait_i) {
    if (fi.frame_type != FRAME_I) return 0;
    r.wait_i = false;
    // 第一帧 I 到达时，重新记录开始时间（更贴近“文件开始时间”）
    r.start_ms_steady = now_ms_steady();
    build_bcdtime8_array(r.start_bcd);
    LOGI("REC ch=%d got first I frame, start recording", ch);
  }

  // 时长保护：超过最大时长立刻停录（事件仍可继续，等待上位机 stop(0x3A) 来取文件信息）
  if (now - r.start_ms_steady >= (int64_t)vmax) {
    r.active = false;
    if (r.fd >= 0) { ::close(r.fd); r.fd = -1; }

    if (!r.finalized && r.file_id != 0) {
      FileInfo25 f;
      f.file_id = r.file_id;
      f.file_len = (uint32_t)r.bytes;
      std::memcpy(f.start_bcd, r.start_bcd, 8);
      f.flag = 0xFF;
      f.duration_ms = vmax;
      f.pos_mask = r.pos_mask4;
      f.reserved = 0;
      f.file_type = uint8_t(((cur_laser_ & 0x01) << 7) | ((cur_light_fill_ & 0x01) << 6) | ((r.cam_id & 0x03) << 4) | 0x01);

      cur_files_.push_back(f);
      r.finalized = true;
      LOGW("REC ch=%d auto-stop at vmax=%ums, file_id=%08X bytes=%llu", ch, vmax, f.file_id, (unsigned long long)r.bytes);
    }
    return 0;
  }

  ssize_t w = ::write(r.fd, buf, (size_t)len);
  if (w > 0) r.bytes += (uint64_t)w;
  return 0;
}


private:
  /**
   * @brief 获取 `MediaManager` 单例指针槽。
   * @return 返回目标对象或引用。
   */
  static MediaManager*& self_ptr() {
    static MediaManager* p = nullptr;
    return p;
  }

  std::mutex mu_;
  bool inited_ = false;
  std::array<RecCtx, 4> rec_{};

  std::atomic<uint32_t> next_event_id_{1};
  std::atomic<uint32_t> next_file_id_{1};

  uint32_t cur_event_id_ = 0;
  std::string cur_event_dir_;
  uint8_t cur_light_fill_ = 0;
  uint8_t cur_laser_ = 0;
  std::vector<FileInfo25> cur_files_;

  RealTimeStreamer* streamer_ = nullptr;
  std::atomic<uint32_t> video_max_ms_{kVideoMaxMs};
};


/**
 * @brief 处理 `handle_snap30_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param media 媒体管理器。
 * @param outbound 输出队列。
 */
inline void handle_snap30_req(const btt::proto::Frame& fr, RuntimeConfig& rt, MediaManager& media,
                              btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  FileInfo25 finfo{};
  uint8_t cur_pos = rt.position.load();
  uint8_t snap_type = 0;

  if (fr.payload.size() != 2) {
    LOGW("0x30 SNAP: bad payload len=%zu (expect 2)", fr.payload.size());
    result = 0x01;
  } else {
    const uint8_t posb = fr.payload[0];
    snap_type = fr.payload[1];

    const uint8_t light_fill = (posb >> 7) & 0x01;
    const uint8_t laser      = (posb >> 6) & 0x01;
    const uint8_t cam_id     = (posb & 0x0F);

    LOGI("0x30 SNAP REQ: posb=0x%02X -> cam=%u fill=%u laser=%u snap_type=%u", posb, cam_id, light_fill, laser, snap_type);

    if (cam_id > 3 || snap_type > 2) {
      result = 0x01;
    } else {
      const uint8_t pos_mask4 = cam_pos_mask4_from_cfg(rt, cam_id);

      uint32_t event_id = 0;
      std::string dir;
      const uint64_t need_bytes = est_event_A_bytes(); // 0x30 只产 1 张，但按 4 张估算上界
      if (!media.snap_jpg_to_event(cam_id, light_fill, laser, pos_mask4, snap_type, need_bytes, event_id, dir, finfo)) {
        result = 0x01;
      } else {
        // 写 meta.bin（供 M4 事件查询使用）
        EventMetaV1 em;
        em.event_id = event_id;
        em.event_type = 2; // 上位机事件
        em.rsv0 = 0;
        em.cur_pos = cur_pos;
        std::memcpy(em.start_bcd, finfo.start_bcd, 8);
        std::memcpy(em.end_bcd, finfo.start_bcd, 8);
        em.hydraulic = 0; em.level = 0; em.temp_humi = 0; em.gap_value = 0;

        std::vector<FileInfo25> vf;
        vf.push_back(finfo);
        if (!write_meta_v1(dir, em, vf)) {
          LOGW("write_meta_v1 failed for SNAP event dir=%s errno=%d", dir.c_str(), errno);
        } else {
          LOGI("SNAP meta written: event_id=%08X dir=%s", event_id, dir.c_str());
        }
      }
    }
  }

  btt::proto::Frame resp;
  resp.cmd = 0x30;
  resp.level = 0x01;
  resp.seq = fr.seq;
  resp.payload.reserve(2 + 25 + 2);
  resp.payload.push_back(result);
  resp.payload.push_back(reason);
  append_fileinfo25(resp.payload, finfo);
  resp.payload.push_back(cur_pos);
  resp.payload.push_back(snap_type);

  outbound.push(std::move(resp));
}

/**
 * @brief 处理 `handle_rec39_start_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param media 媒体管理器。
 * @param outbound 输出队列。
 */
inline void handle_rec39_start_req(const btt::proto::Frame& fr, RuntimeConfig& rt, MediaManager& media,
                                   btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;
  const uint8_t cur_pos = rt.position.load();

  uint32_t event_id = 0;
  std::string dir;

  if (fr.payload.size() != 1) {
    LOGW("0x39 REC_START: bad payload len=%zu (expect 1)", fr.payload.size());
    result = 0x01;
  } else {
    const uint8_t posb = fr.payload[0];
    const uint8_t light_fill = (posb >> 7) & 0x01;
    const uint8_t laser      = (posb >> 6) & 0x01;
    const uint8_t cam_sel    = (posb & 0x0F);
    const bool all = (cam_sel == 0x09);

    LOGI("0x39 REC_START REQ: posb=0x%02X -> cam_sel=%u all=%d fill=%u laser=%u",
         posb, cam_sel, all ? 1 : 0, light_fill, laser);

    if (!(all || cam_sel <= 3)) {
      result = 0x01;
    } else {
      const uint32_t kbps = rt.video_bitrate_kbps.load();
      if (kbps > 1024u) {
        result = 0x01;
        reason = 0x02; // bitrate over limit
      } else {
        const uint64_t need_bytes = est_event_B_bytes(kbps);
        if (!media.start_record(cam_sel, all, light_fill, laser, rt, need_bytes, event_id, dir)) {
          result = 0x01;
        } else {
          LOGI("0x39 REC_START OK: event_id=%08X dir=%s", event_id, dir.c_str());

          // 写 meta.bin（先写空文件列表；0x3A 停止后再补齐）
          EventMetaV1 em;
          em.event_id = event_id;
          em.event_type = 2; // 上位机事件
          em.rsv0 = 0;
          em.cur_pos = cur_pos;
          build_bcdtime8_array(em.start_bcd);
          std::memset(em.end_bcd, 0, 8);
          em.hydraulic = 0; em.level = 0; em.temp_humi = 0; em.gap_value = 0;

          std::vector<FileInfo25> empty;
          if (!write_meta_v1(dir, em, empty)) {
            LOGW("write_meta_v1 failed for REC_START dir=%s errno=%d", dir.c_str(), errno);
          }
        }
      }
    }
  }

  btt::proto::Frame resp;
  resp.cmd = 0x39;
  resp.level = 0x01;
  resp.seq = fr.seq;
  resp.payload = {result, reason, cur_pos};
  outbound.push(std::move(resp));
}

/**
 * @brief 处理 `handle_rec3A_stop_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param media 媒体管理器。
 * @param outbound 输出队列。
 */
inline void handle_rec3A_stop_req(const btt::proto::Frame& fr, RuntimeConfig& rt, MediaManager& media,
                                  btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;
  std::vector<FileInfo25> files;

  if (fr.payload.size() != 1) {
    LOGW("0x3A REC_STOP: bad payload len=%zu (expect 1)", fr.payload.size());
    result = 0x01;
  } else {
    const uint8_t posb = fr.payload[0];
    const uint8_t light_fill = (posb >> 7) & 0x01;
    const uint8_t laser      = (posb >> 6) & 0x01;
    const uint8_t cam_sel    = (posb & 0x0F);
    const bool all = (cam_sel == 0x09);

    LOGI("0x3A REC_STOP REQ: posb=0x%02X -> cam_sel=%u all=%d fill=%u laser=%u", posb, cam_sel, all ? 1 : 0, light_fill, laser);

    if (!(all || cam_sel <= 3)) {
      result = 0x01;
    } else {
      if (!media.stop_record(cam_sel, all, light_fill, laser, files)) {
        result = 0x01;
      }
    }
  }


// 录像停止成功后，补齐 meta.bin（包含文件列表 + 结束时间）
if (result == 0x00 && !files.empty()) {
  uint32_t eid = 0;
  std::string edir;
  uint8_t lf = 0, lz = 0;
  if (media.current_event(eid, edir, lf, lz)) {
    EventMetaV1 em;
    std::vector<FileInfo25> old_files;
    if (!read_meta_v1(edir, em, old_files)) {
      // meta 不存在也没关系：用当前补齐
      em.event_id = eid;
      em.event_type = 2;
      em.rsv0 = 0;
      em.cur_pos = rt.position.load();
      if (!files.empty()) std::memcpy(em.start_bcd, files[0].start_bcd, 8);
    }
    build_bcdtime8_array(em.end_bcd);

    if (!write_meta_v1(edir, em, files)) {
      LOGW("write_meta_v1 failed for REC_STOP dir=%s errno=%d", edir.c_str(), errno);
    } else {
      LOGI("REC_STOP meta written: event_id=%08X dir=%s files=%zu", eid, edir.c_str(), files.size());
    }
    media.clear_current_event();
  } else {
    LOGW("REC_STOP: current_event not available, skip meta write");
  }
}

  btt::proto::Frame resp;
  resp.cmd = 0x3A;
  resp.level = 0x01;
  resp.seq = fr.seq;

  resp.payload.push_back(result);
  resp.payload.push_back(reason);
  resp.payload.push_back(uint8_t(files.size() & 0xFF));
  for (const auto& f : files) append_fileinfo25(resp.payload, f);

  outbound.push(std::move(resp));
}

inline uint32_t read_u32_be(const uint8_t* p);
inline uint16_t read_u16_be(const uint8_t* p);

// ---------- M7：实时流 0x35/0x36/0x37/0x51 ----------
/**
 * @brief 处理 `handle_stream_open_35_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param media 媒体管理器。
 * @param streamer 实时流管理器。
 * @param outbound 输出队列。
 */
inline void handle_stream_open_35_req(const btt::proto::Frame& fr, RuntimeConfig& rt, MediaManager& media,
                                      RealTimeStreamer& streamer,
                                      btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  uint8_t cam_id = 0;
  uint32_t ip_be = 0;
  uint16_t port = 0;

  if (fr.payload.size() != 7) {
    LOGW("0x35 STREAM_OPEN: bad payload len=%zu (expect 7)", fr.payload.size());
    result = 0x01;
    reason = 0x01;
  } else {
    cam_id = fr.payload[0];
    ip_be = read_u32_be(fr.payload.data() + 1);
    port = read_u16_be(fr.payload.data() + 5);

    LOGI("0x35 STREAM_OPEN REQ: cam=%u ip=%s port=%u", cam_id, ip_be_to_string(ip_be).c_str(), port);

    if (cam_id > 3 || port == 0) {
      result = 0x01;
      reason = 0x01;
    } else if (streamer.active()) {
      if (streamer.match_params(cam_id, ip_be, port)) {
        streamer.keepalive();
      } else {
        result = 0x01;
        reason = 0x02; // busy
      }
    } else {
      if (!media.init_once()) {
        result = 0x01;
        reason = 0x03;
      } else if (!streamer.start(cam_id, ip_be, port)) {
        result = 0x01;
        reason = 0x03;
      } else {
        streamer.keepalive();
        LOGI("0x35 STREAM_OPEN OK: cam=%u -> %s:%u", cam_id, ip_be_to_string(ip_be).c_str(), port);
      }
    }
  }

  uint8_t cur_cam = streamer.active() ? streamer.cam_id() : cam_id;
  btt::proto::Frame resp;
  resp.cmd = 0x35;
  resp.level = 0x01;
  resp.seq = fr.seq;
  resp.payload.push_back(result);
  resp.payload.push_back(reason);
  resp.payload.push_back(cur_cam);
  outbound.push(std::move(resp));
}

/**
 * @brief 处理 `handle_stream_close_36_req` 对应流程。
 * @param fr 协议帧。
 * @param streamer 实时流管理器。
 * @param outbound 输出队列。
 */
inline void handle_stream_close_36_req(const btt::proto::Frame& fr, RealTimeStreamer& streamer,
                                       btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  uint8_t cam_id = 0;
  if (fr.payload.size() != 1) {
    LOGW("0x36 STREAM_CLOSE: bad payload len=%zu (expect 1)", fr.payload.size());
    result = 0x01;
    reason = 0x01;
  } else {
    cam_id = fr.payload[0];
    LOGI("0x36 STREAM_CLOSE REQ: cam=%u", cam_id);
    if (cam_id > 3) {
      result = 0x01;
      reason = 0x01;
    } else if (!streamer.active() || streamer.cam_id() != cam_id) {
      result = 0x01;
      reason = 0x02;
    } else {
      streamer.close_by_host(cam_id);
      LOGI("0x36 STREAM_CLOSE OK: cam=%u", cam_id);
    }
  }

  btt::proto::Frame resp;
  resp.cmd = 0x36;
  resp.level = 0x01;
  resp.seq = fr.seq;
  resp.payload.push_back(result);
  resp.payload.push_back(reason);
  outbound.push(std::move(resp));
}

/**
 * @brief 处理 `handle_stream_end_37_resp` 对应流程。
 * @param fr 协议帧。
 */
inline void handle_stream_end_37_resp(const btt::proto::Frame& fr) {
  if (fr.payload.size() < 2) {
    LOGW("0x37 STREAM_END ACK: bad payload len=%zu", fr.payload.size());
    return;
  }
  const uint8_t result = fr.payload[0];
  const uint8_t reason = fr.payload[1];
  LOGI("0x37 STREAM_END ACK: result=%u reason=0x%02X", result, reason);
}



// ---------- M5：网络/媒体/测量 0x11/0x12/0x1A/0x1B/0x38 + 0x5B 上报 ----------
inline uint32_t read_u32_be(const uint8_t* p);
inline uint16_t read_u16_be(const uint8_t* p);
inline void append_u16_be(std::vector<uint8_t>& out, uint16_t v);


/**
 * @brief 向输出缓冲区追加 `append_ip_be` 对应内容。
 * @param out 输出对象。
 * @param ip_be 大端 IPv4 地址。
 */
inline void append_ip_be(std::vector<uint8_t>& out, uint32_t ip_be) {
  append_u32_be(out, ip_be);
}

struct FactoryResetSnapshot {
  M5PersistV1 m5_cfg{};
  bool measure_periodic = false;
  bool devcfg_valid = false;
  std::array<uint8_t, kDevCfgLen> devcfg_raw{};
  uint8_t switch_model = 0x01;
  uint8_t hb_interval_s = kFactoryHbIntervalS;
  uint8_t business_timeout_s = kFactoryBusinessTimeoutS;
  uint8_t reconnect_interval_s = kFactoryReconnectIntervalS;
  uint8_t no_comm_reboot_s = kFactoryNoCommRebootS;
};

/**
 * @brief 快照保存 `snapshot_factory_reset_state` 对应状态。
 * @param rt 共享运行态。
 * @param out 输出对象。
 */
inline void snapshot_factory_reset_state(const RuntimeConfig& rt,
                                         FactoryResetSnapshot& out) {
  m5_snapshot_runtime_to_persist(rt, out.m5_cfg);
  out.measure_periodic = rt.m5_measure_periodic.load();
  out.switch_model = rt.switch_model.load();
  out.hb_interval_s = rt.hb_interval_s.load();
  out.business_timeout_s = rt.business_timeout_s.load();
  out.reconnect_interval_s = rt.reconnect_interval_s.load();
  out.no_comm_reboot_s = rt.no_comm_reboot_s.load();
  out.devcfg_valid = rt.devcfg_valid.load();
  std::lock_guard<std::mutex> lk(rt.devcfg_mu);
  out.devcfg_raw = rt.devcfg_raw;
}

/**
 * @brief 恢复 `restore_factory_reset_state` 对应状态。
 * @param snapshot 状态快照。
 * @param rt 共享运行态。
 */
inline void restore_factory_reset_state(const FactoryResetSnapshot& snapshot,
                                        RuntimeConfig& rt) {
  const M5PersistV1& cfg = snapshot.m5_cfg;

  rt.m5_dev_ip_be.store(cfg.dev_ip_be);
  rt.m5_gateway_be.store(cfg.gateway_be);
  rt.m5_netmask_be.store(cfg.netmask_be);
  rt.m5_host_ip_be.store(cfg.host_ip_be);
  rt.m5_host_port.store(cfg.host_port);
  rt.m5_upgrade_server_ip_be.store(cfg.upgrade_ip_be);
  rt.m5_debug_server_ip_be.store(cfg.debug_ip_be);
  rt.m5_debug_server_port.store(cfg.debug_port);
  rt.m5_debug_proto.store(cfg.debug_proto);
  rt.m5_pic_res.store(cfg.pic_res);
  rt.m5_pic_quality.store(cfg.pic_quality);
  rt.m5_video_res.store(cfg.video_res);
  rt.m5_video_fps.store(cfg.video_fps);
  rt.m5_video_bitrate_unit.store(cfg.video_bitrate_unit);
  rt.video_bitrate_kbps.store(bitrate_unit_to_kbps(cfg.video_bitrate_unit));
  rt.m5_measure_interval_min.store(cfg.measure_interval_min);
  rt.m5_measure_periodic.store(snapshot.measure_periodic);
  rt.m8_switch_trigger.store(cfg.m8_switch_trigger);
  rt.m8_pass_trigger.store(cfg.m8_pass_trigger);
  {
    std::lock_guard<std::mutex> lk(rt.m8_mu);
    std::memcpy(rt.m8_strategy_raw0.data(), cfg.m8_strategy_raw0, 73);
    std::memcpy(rt.m8_strategy_raw1.data(), cfg.m8_strategy_raw1, 73);
  }
  m8_apply_effective_from_strategy_raw(0, cfg.m8_strategy_raw0, rt);
  m8_apply_effective_from_strategy_raw(1, cfg.m8_strategy_raw1, rt);

  rt.switch_model.store(snapshot.switch_model);
  rt.hb_interval_s.store(snapshot.hb_interval_s);
  rt.business_timeout_s.store(snapshot.business_timeout_s);
  rt.reconnect_interval_s.store(snapshot.reconnect_interval_s);
  rt.no_comm_reboot_s.store(snapshot.no_comm_reboot_s);

  std::lock_guard<std::mutex> lk(rt.devcfg_mu);
  rt.devcfg_raw = snapshot.devcfg_raw;
  rt.devcfg_valid.store(snapshot.devcfg_valid);
}

/**
 * @brief 应用 `apply_factory_defaults` 对应配置。
 * @param def 启动默认配置。
 * @param rt 共享运行态。
 * @param media 媒体管理器。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool apply_factory_defaults(const DefaultConfig& def, RuntimeConfig& rt,
                                   MediaManager& media) {
  FactoryResetSnapshot snapshot{};
  snapshot_factory_reset_state(rt, snapshot);

  uint8_t raw0[73]{};
  uint8_t raw1[73]{};
  m8_fill_default_strategy_raw(0, raw0);
  m8_fill_default_strategy_raw(1, raw1);

  rt.m5_dev_ip_be.store(kFactoryDevIpBe);
  rt.m5_gateway_be.store(kFactoryGatewayIpBe);
  rt.m5_netmask_be.store(kFactoryNetmaskBe);
  rt.m5_host_ip_be.store(kFactoryServerIpBe);
  rt.m5_host_port.store(def.server_port);
  rt.m5_pic_res.store(kFactoryMediaPicRes);
  rt.m5_pic_quality.store(kFactoryMediaPicQuality);
  rt.m5_video_res.store(kFactoryMediaVideoRes);
  rt.m5_video_fps.store(kFactoryMediaVideoFps);
  rt.m5_video_bitrate_unit.store(kFactoryMediaVideoBitrateUnit);
  rt.video_bitrate_kbps.store(bitrate_unit_to_kbps(kFactoryMediaVideoBitrateUnit));
  rt.m5_measure_periodic.store(false);
  rt.m5_measure_interval_min.store(kFactoryMeasureIntervalMin);

  rt.switch_model.store(def.switch_model);
  rt.hb_interval_s.store(kFactoryHbIntervalS);
  rt.business_timeout_s.store(kFactoryBusinessTimeoutS);
  rt.reconnect_interval_s.store(kFactoryReconnectIntervalS);
  rt.no_comm_reboot_s.store(kFactoryNoCommRebootS);

  {
    std::lock_guard<std::mutex> lk(rt.devcfg_mu);
    rt.devcfg_raw.fill(0);
    rt.devcfg_valid.store(false);
  }

  rt.m8_switch_trigger.store(kFactoryTriggerMode);
  rt.m8_pass_trigger.store(kFactoryTriggerMode);
  {
    std::lock_guard<std::mutex> lk(rt.m8_mu);
    std::memcpy(rt.m8_strategy_raw0.data(), raw0, 73);
    std::memcpy(rt.m8_strategy_raw1.data(), raw1, 73);
  }
  m8_apply_effective_from_strategy_raw(0, raw0, rt);
  m8_apply_effective_from_strategy_raw(1, raw1, rt);

  if (!media.apply_video_params_from_rt(rt)) {
    restore_factory_reset_state(snapshot, rt);
    (void)media.apply_video_params_from_rt(rt);
    return false;
  }

  if (!m5_persist_save(rt)) {
    restore_factory_reset_state(snapshot, rt);
    (void)media.apply_video_params_from_rt(rt);
    return false;
  }

  LOGI("0x16 FACTORY defaults applied: host=%s:%u dev=%s gw=%s mask=%s",
       ip_be_to_string(rt.m5_host_ip_be.load()).c_str(), rt.m5_host_port.load(),
       ip_be_to_string(rt.m5_dev_ip_be.load()).c_str(),
       ip_be_to_string(rt.m5_gateway_be.load()).c_str(),
       ip_be_to_string(rt.m5_netmask_be.load()).c_str());
  return true;
}

/**
 * @brief 处理 `handle_control_16_req` 对应流程。
 * @param fr 协议帧。
 * @param def 启动默认配置。
 * @param rt 共享运行态。
 * @param media 媒体管理器。
 * @param pending_control_action `pending_control_action` 参数。
 * @param pending_control_seq `pending_control_seq` 参数。
 * @param outbound 输出队列。
 */
inline void handle_control_16_req(const btt::proto::Frame& fr,
                                  const DefaultConfig& def,
                                  RuntimeConfig& rt,
                                  MediaManager& media,
                                  std::atomic<uint8_t>& pending_control_action,
                                  std::atomic<uint16_t>& pending_control_seq,
                                  btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;
  CoreControlAction action = CoreControlAction::None;

  if (pending_control_action.load() !=
      static_cast<uint8_t>(CoreControlAction::None)) {
    result = 0x01;
  } else if (fr.payload.size() != 1) {
    result = 0x01;
  } else if (fr.payload[0] == 0x00) {
    action = CoreControlAction::RebootDevice;
  } else if (fr.payload[0] == 0x01) {
    if (!apply_factory_defaults(def, rt, media)) {
      result = 0x01;
    } else {
      action = CoreControlAction::RestartProgram;
    }
  } else {
    result = 0x01;
  }

  if (result == 0x00) {
    pending_control_action.store(static_cast<uint8_t>(action));
    pending_control_seq.store(fr.seq);
    LOGI("0x16 CONTROL accepted: op=%u seq=%u action=%u",
         unsigned(fr.payload[0]), fr.seq, unsigned(pending_control_action.load()));
  } else {
    pending_control_action.store(static_cast<uint8_t>(CoreControlAction::None));
    pending_control_seq.store(0);
    LOGW("0x16 CONTROL rejected: payload_len=%zu op=%u seq=%u",
         fr.payload.size(),
         fr.payload.empty() ? 0xFFu : unsigned(fr.payload[0]),
         fr.seq);
  }

  outbound.push(make_resp_2b(0x16, fr.seq, result, reason));
}

struct PendingNetConfig {
  uint32_t dev_ip = 0;
  uint32_t gateway = 0;
  uint32_t netmask = 0;
  uint32_t host_ip = 0;
  uint16_t host_port = 0;
  uint32_t upgrade_ip = 0;
  uint32_t debug_ip = 0;
  uint16_t debug_port = 0;
  uint8_t debug_proto = 0;
  bool local_net_changed = false;
  bool need_disconnect = false;
};

/**
 * @brief 执行 `commit_pending_net_config_after_ack` 内部辅助逻辑。
 * @param cfg 待提交配置。
 * @param rt 共享运行态。
 */
inline void commit_pending_net_config_after_ack(const PendingNetConfig& cfg,
                                                RuntimeConfig& rt) {
  if (cfg.local_net_changed) {
    std::string err;
    if (!apply_net_config_ioctl("eth0", cfg.dev_ip, cfg.netmask, cfg.gateway,
                                err)) {
      LOGW("0x11 SET_NET post-ack apply failed: %s", err.c_str());
      return;
    }
  }

  rt.m5_dev_ip_be.store(cfg.dev_ip);
  rt.m5_gateway_be.store(cfg.gateway);
  rt.m5_netmask_be.store(cfg.netmask);
  rt.m5_host_ip_be.store(cfg.host_ip);
  rt.m5_host_port.store(cfg.host_port);
  rt.m5_upgrade_server_ip_be.store(cfg.upgrade_ip);
  rt.m5_debug_server_ip_be.store(cfg.debug_ip);
  rt.m5_debug_server_port.store(cfg.debug_port);
  rt.m5_debug_proto.store(cfg.debug_proto);

  if (!m5_persist_save(rt)) {
    LOGW("0x11 SET_NET post-ack persist failed");
  }

  if (cfg.need_disconnect) {
    rt.force_disconnect_reason.store(18);
    rt.force_disconnect.store(true);
  }

  LOGI("0x11 SET_NET APPLIED after ACK: dev=%s gw=%s mask=%s host=%s:%u",
       ip_be_to_string(cfg.dev_ip).c_str(),
       ip_be_to_string(cfg.gateway).c_str(),
       ip_be_to_string(cfg.netmask).c_str(),
       ip_be_to_string(cfg.host_ip).c_str(),
       cfg.host_port);
}

/**
 * @brief 处理 `handle_set_netparam_11_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param pending_net_mu `pending_net_mu` 参数。
 * @param pending_net_cfg `pending_net_cfg` 参数。
 * @param pending_net_active `pending_net_active` 参数。
 * @param pending_net_seq `pending_net_seq` 参数。
 * @param outbound 输出队列。
 */
inline void handle_set_netparam_11_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                      std::mutex& pending_net_mu,
                                      PendingNetConfig& pending_net_cfg,
                                      std::atomic<bool>& pending_net_active,
                                      std::atomic<uint16_t>& pending_net_seq,
                                      btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  if (pending_net_active.load()) {
    result = 0x01;
    reason = 0x01;
    LOGW("0x11 SET_NET rejected: previous network update still pending");
  } else if (fr.payload.size() != 29) {
    LOGW("0x11 SET_NET: bad payload len=%zu (expect 29)", fr.payload.size());
    result = 0x01; reason = 0x01;
  } else {
    const uint8_t* p = fr.payload.data();
    const uint32_t dev_ip   = read_u32_be(p + 0);
    const uint32_t gw       = read_u32_be(p + 4);
    const uint32_t mask     = read_u32_be(p + 8);
    const uint32_t host_ip  = read_u32_be(p + 12);
    const uint16_t host_pt  = read_u16_be(p + 16);
    const uint32_t upg_ip   = read_u32_be(p + 18);
    const uint32_t dbg_ip   = read_u32_be(p + 22);
    const uint16_t dbg_pt  = read_u16_be(p + 26);
    const uint8_t  dbg_pr = p[28];

    if (dbg_pr > 1 || host_pt == 0) {
      result = 0x01; reason = 0x01;
    } else {
      PendingNetConfig staged{};
      staged.dev_ip = dev_ip;
      staged.gateway = gw;
      staged.netmask = mask;
      staged.host_ip = host_ip;
      staged.host_port = host_pt;
      staged.upgrade_ip = upg_ip;
      staged.debug_ip = dbg_ip;
      staged.debug_port = dbg_pt;
      staged.debug_proto = dbg_pr;
      staged.local_net_changed =
          (rt.m5_dev_ip_be.load() != dev_ip) ||
          (rt.m5_gateway_be.load() != gw) ||
          (rt.m5_netmask_be.load() != mask);
      staged.need_disconnect =
          staged.local_net_changed ||
          (rt.m5_host_ip_be.load() != host_ip) ||
          (rt.m5_host_port.load() != host_pt);

      {
        std::lock_guard<std::mutex> lk(pending_net_mu);
        pending_net_cfg = staged;
      }
      pending_net_seq.store(fr.seq);
      pending_net_active.store(true);

      LOGI("0x11 SET_NET staged before ACK: dev=%s gw=%s mask=%s host=%s:%u need_disconnect=%u",
           ip_be_to_string(dev_ip).c_str(),
           ip_be_to_string(gw).c_str(),
           ip_be_to_string(mask).c_str(),
           ip_be_to_string(host_ip).c_str(),
           host_pt,
           staged.need_disconnect ? 1u : 0u);
    }
  }

  btt::proto::Frame resp;
  resp.cmd = 0x11;
  resp.level = 0x01;
  resp.seq = fr.seq;
  resp.payload = {result, reason};
  outbound.push(std::move(resp));
}

/**
 * @brief 处理 `handle_get_netparam_12_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param outbound 输出队列。
 */
inline void handle_get_netparam_12_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                      btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;
  (void)fr;

  uint32_t ip=0, mask=0, gw=0;
  bool ok1 = get_ipv4_and_mask_host("eth0", ip, mask);
  bool ok2 = get_default_gateway_host("eth0", gw);
  if (ok1) {
    rt.m5_dev_ip_be.store( (ip) );
    rt.m5_netmask_be.store( (mask) );
  }
  if (ok2) {
    rt.m5_gateway_be.store( (gw) );
  }


  btt::proto::Frame resp;
  resp.cmd = 0x12;
  resp.level = 0x01;
  resp.seq = fr.seq;

  resp.payload.reserve(64);
  resp.payload.push_back(result);
  resp.payload.push_back(reason);
  append_ip_be(resp.payload, rt.m5_dev_ip_be.load());
  append_ip_be(resp.payload, rt.m5_gateway_be.load());
  append_ip_be(resp.payload, rt.m5_netmask_be.load());
  append_ip_be(resp.payload, rt.m5_host_ip_be.load());
  append_u16_be(resp.payload, rt.m5_host_port.load());
  append_ip_be(resp.payload, rt.m5_upgrade_server_ip_be.load());
  append_ip_be(resp.payload, rt.m5_debug_server_ip_be.load());
  append_u16_be(resp.payload, rt.m5_debug_server_port.load());
  resp.payload.push_back(rt.m5_debug_proto.load());

  outbound.push(std::move(resp));
}

/**
 * @brief 处理 `handle_set_media_1A_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param media 媒体管理器。
 * @param outbound 输出队列。
 */
inline void handle_set_media_1A_req(const btt::proto::Frame& fr, RuntimeConfig& rt, MediaManager& media,
                                   btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  if (fr.payload.size() != 9) {
    LOGW("0x1A SET_MEDIA: bad payload len=%zu (expect 9)", fr.payload.size());
    result = 0x01; reason = 0x01;
  } else {
    // 0x1A payload[0] = 图片分辨率（无效化）
    // uint8_t pic_res = p[0];

    const uint8_t pic_q   = fr.payload[1];
    const uint8_t vid_res = fr.payload[2];
    const uint8_t vid_fps = fr.payload[3];
    const uint8_t vid_br  = fr.payload[4];

    hif_pic_size tmp ;
    if ( !map_res_code_to_hif(vid_res, tmp) ||
        pic_q < 5 || pic_q > 90 ||
        vid_fps < 1 || vid_fps > 25 ||
        vid_br == 0 || vid_br > 32) {
      result = 0x01; reason = 0x01;
    } else {
      rt.m5_pic_res.store(vid_res); // 图片分辨率同录像分辨率
      rt.m5_pic_quality.store(pic_q);
      rt.m5_video_res.store(vid_res);
      rt.m5_video_fps.store(vid_fps);
      rt.m5_video_bitrate_unit.store(vid_br);
      rt.video_bitrate_kbps.store(bitrate_unit_to_kbps(vid_br));

      (void)m5_persist_save(rt);

      // 尝试即时生效（对录像编码参数）
      if (!media.apply_video_params_from_rt(rt)) {
        // 不强制失败：底层库可能不支持运行时切换
        LOGW("0x1A SET_MEDIA: apply_video_params failed, keep runtime only");
      }

      LOGI("0x1A SET_MEDIA OK: pic_res=%u q=%u vid_res=%u fps=%u br_unit=%u(%ukbps)",
           vid_res, pic_q, vid_res, vid_fps, vid_br,
           unsigned(bitrate_unit_to_kbps(vid_br)));
    }
  }

  outbound.push(make_resp_2b(0x1A, fr.seq, result, reason));
}
/**
 * @brief 执行 `hif_pic_to_code` 内部辅助逻辑。
 * @param hif_pic `hif_pic` 参数。
 * @return 返回计算结果。
 */
static uint8_t hif_pic_to_code(int hif_pic)
{
  switch (hif_pic) {
    case HIF_PIC_D1_NTSC: return 3;
    case HIF_PIC_D1_PAL:  return 6;
    case HIF_PIC_720P:    return 7;
    case HIF_PIC_1080P:   return 8;
    default:              return 3; // 或 0xFF 表示未知
  }
}

/**
 * @brief 判断 `is_media_res_code_valid` 条件是否成立。
 * @param code 枚举或配置编码值。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool is_media_res_code_valid(uint8_t code) {
  return code == 3 || code == 6 || code == 7 || code == 8;
}

static int     code_to_hif_pic(uint8_t code);  // 也需要 0x1A 用

/**
 * @brief 处理 `handle_get_media_1B_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param outbound 输出队列。
 */
void handle_get_media_1B_req(const btt::proto::Frame& fr,
                            btt::core::RuntimeConfig& rt,
                            btt::utils::BlockingQueue<btt::proto::Frame>& outbound)
{
  (void)fr;
  btt::proto::Frame resp;
  resp.cmd = 0x1B;
  resp.level = 0x01;
  resp.seq = fr.seq;

  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  // 优先回协议/runtime 配置，避免底层库字段语义和协议不一致
  uint8_t pic_res_code = rt.m5_pic_res.load();
  uint8_t pic_quality  = rt.m5_pic_quality.load();
  uint8_t vid_res_code = rt.m5_video_res.load();
  uint8_t vid_fps      = rt.m5_video_fps.load();
  uint8_t vid_br_unit  = rt.m5_video_bitrate_unit.load();

  if (!is_media_res_code_valid(pic_res_code)) {
    pic_res_code = kFactoryMediaPicRes;
  }
  if (!is_media_res_code_valid(vid_res_code)) {
    vid_res_code = kFactoryMediaVideoRes;
  }
  if (pic_quality < 5 || pic_quality > 90) pic_quality = kFactoryMediaPicQuality;
  if (vid_fps == 0 || vid_fps > 25) vid_fps = kFactoryMediaVideoFps;
  if (vid_br_unit == 0 || vid_br_unit > 32) vid_br_unit = kFactoryMediaVideoBitrateUnit;

  // 1) 从 HiF 读取参数
  hif_media_param_t mp;  
  std::memset(&mp, 0, sizeof(mp));

  int rc = HiF_media_get_param(0,&mp);  
  if (rc != 0) {
    result = 0x01;
    reason = 0x01;  // 读取失败原因码自定义
  } else {
    // 分辨率优先用底层当前编码器状态，其余字段回 runtime 配置
    const uint8_t hif_res_code = hif_pic_to_code(mp.pic_size);
    if (hif_res_code == 3 || hif_res_code == 6 || hif_res_code == 7 || hif_res_code == 8) {
      pic_res_code = hif_res_code;
      vid_res_code = hif_res_code;
    }
  }
  printf("get_media_1B: hif_res=%u hif_fps=%d hif_bit_rate=%d rt_fps=%u rt_br_unit=%u\n",
         mp.pic_size, mp.frame_rate, mp.bit_rate, vid_fps, vid_br_unit);

  // 3) 组包 payload

  resp.payload = {
  
  0x00, 0x00,
  pic_res_code,
  pic_quality,
  vid_res_code,
  vid_fps,
  vid_br_unit,
  0x00,
  0x00,
  0x00,
  0x00
  };
  outbound.push(std::move(resp));
}

/**
 * @brief 处理 `handle_measure_38_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param media 媒体管理器。
 * @param outbound 输出队列。
 */
inline void handle_measure_38_req(const btt::proto::Frame& fr, RuntimeConfig& rt, MediaManager& media,
                                 btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  if (fr.payload.size() != 2) {
    result = 0x01; reason = 0x01;
  } else {
    const uint8_t mode = fr.payload[0];
    const uint8_t interval_min = fr.payload[1];

    if (mode == 0) {
      // 周期测量（配置）
      if (interval_min == 0) { result = 0x01; reason = 0x01; }
      else {
        rt.m5_measure_interval_min.store(interval_min);
        rt.m5_measure_periodic.store(true);
        (void)m5_persist_save(rt);
        LOGI("0x38 PERIODIC_MEAS configured: interval=%umin", interval_min);
      }

      outbound.push(make_resp_2b(0x38, fr.seq, result, reason));
      return;
    }

    if (mode != 1) {
      result = 0x01; reason = 0x01;
      outbound.push(make_resp_2b(0x38, fr.seq, result, reason));
      return;
    }

    // 手工测量：立即抓拍 4 路并在应答里返回结构
    uint32_t event_id = 0;
    std::string dir;
    std::vector<FileInfo25> files;

    const uint64_t need_bytes = est_event_A_bytes();
    if (!media.measure_snap4_to_event(rt, 0, 0, need_bytes, event_id, dir, files)) {
      result = 0x01; reason = 0x02;
    } else {
      // 写 meta.bin，便于后续 M4 查询
      EventMetaV1 em;
      em.event_id = event_id;
      em.event_type = 3; // 约定：3=测量事件
      em.rsv0 = 0;
      em.cur_pos = rt.position.load();
      if (!files.empty()) {
        std::memcpy(em.start_bcd, files[0].start_bcd, 8);
        std::memcpy(em.end_bcd, files[0].start_bcd, 8);
      }
      em.hydraulic = 0xFFFF;
      em.level = 0xFFFF;
      em.temp_humi = 0xFFFFFFFFu;
      em.gap_value = 0xFFFFFFFFu;
      (void)write_meta_v1(dir, em, files);
    }

    btt::proto::Frame resp;
    resp.cmd = 0x38;
    resp.level = 0x01;
    resp.seq = fr.seq;

    resp.payload.reserve(160);
    resp.payload.push_back(result);
    resp.payload.push_back(reason);
    resp.payload.push_back(mode);
    resp.payload.push_back(rt.position.load());

    uint8_t t8[8]{};
    build_bcdtime8_array(t8);
    resp.payload.insert(resp.payload.end(), t8, t8 + 8);

    // hydraulic/level/temp_humi/gap_value 默认 FF
    append_u16_be(resp.payload, 0xFFFF);
    append_u16_be(resp.payload, 0xFFFF);
    append_u32_be(resp.payload, 0xFFFFFFFFu);
    append_u32_be(resp.payload, 0xFFFFFFFFu);

    for (const auto& f : files) append_fileinfo25(resp.payload, f);

    outbound.push(std::move(resp));
    return;
  }

  outbound.push(make_resp_2b(0x38, fr.seq, result, reason));
}

/**
 * @brief 构造 `make_measure_event_5B_req` 对应结果。
 * @param rt 共享运行态。
 * @param meas_type `meas_type` 参数。
 * @param action_type `action_type` 参数。
 * @param event_id 事件 ID。
 * @param files 文件信息列表。
 * @return 返回构造后的协议帧。
 */
inline btt::proto::Frame make_measure_event_5B_req(RuntimeConfig& rt,
                                                   uint8_t meas_type, uint8_t action_type,
                                                   uint32_t event_id,
                                                   const std::vector<FileInfo25>& files) {
  btt::proto::Frame f;
  f.cmd = 0x5B;
  f.level = 0x00; // 设备->上位机 上报(请求)
  f.seq = rt.seq.fetch_add(1);

  f.payload.reserve(64 + files.size() * 25);
  f.payload.push_back(meas_type);
  f.payload.push_back(action_type);
  append_u32_be(f.payload, event_id);

  uint8_t t8[8]{};
  build_bcdtime8_array(t8);
  f.payload.insert(f.payload.end(), t8, t8 + 8);

  append_u32_be(f.payload, 0xFFFFFFFFu); // 温湿度默认
  f.payload.push_back(0x03); // 位置信息，不确定，先写死 3=测量事件位置
  append_u16_be(f.payload, 0xFFFF);
  append_u16_be(f.payload, 0xFFFF);
  append_u16_be(f.payload, 0xFFFF);
  append_u16_be(f.payload, 0xFFFF); // 保留 2B

  f.payload.push_back(uint8_t(files.size() & 0xFF));
  for (const auto& fi : files) append_fileinfo25(f.payload, fi);
  return f;
}

/**
 * @brief 处理 `handle_measure_event_5B_resp` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 */
inline void handle_measure_event_5B_resp(const btt::proto::Frame& fr, RuntimeConfig& rt) {
  (void)rt;
  if (fr.payload.size() < 2) return;
  const uint8_t result = fr.payload[0];
  const uint8_t reason = fr.payload[1];
  if (result == 0x00) LOGI("0x5B ACK: seq=%u result=%u reason=%u", fr.seq, result, reason);
  else                LOGW("0x5B ACK FAIL: seq=%u result=%u reason=%u", fr.seq, result, reason);
}

// ---------- M4：事件管理 0x3D(事件列表) / 0x3E(事件详情) / 0x3F(事件清除) ----------
// 存储约定：/data/events/<event_id>/meta.bin + 若干媒体文件
// meta.bin（V1，二进制，小而稳定）：
//  0..3   : 'E''V''T''1'
//  4      : ver=1
//  5      : reserved
//  6      : event_type
//  7      : cur_pos
//  8..11  : event_id (BE)
//  12..19 : start_bcd8
//  20..27 : end_bcd8
//  28..29 : hydraulic (BE)
//  30..31 : level (BE)
//  32..35 : temp_humi (BE)
//  36..39 : gap_value (BE)
//  40     : file_count
//  41..43 : reserved
//  44..   : file_count * FileInfo25
//  tail-4 : crc32(IEEE) over bytes[0..tail-5]
static constexpr const char* kMetaName = "meta.bin";

/**
 * @brief 执行 `crc32_ieee` 内部辅助逻辑。
 * @param data 输入数据。
 * @param n 字节数。
 * @return 返回计算结果。
 */
inline uint32_t crc32_ieee(const uint8_t* data, size_t n) {
  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < n; ++i) {
    crc ^= (uint32_t)data[i];
    for (int k = 0; k < 8; ++k) {
      const uint32_t mask = (crc & 1u) ? 0xEDB88320u : 0u;
      crc = (crc >> 1) ^ mask;
    }
  }
  return ~crc;
}

/**
 * @brief 向输出缓冲区追加 `append_u16_be` 对应内容。
 * @param out 输出对象。
 * @param v 输入数值。
 */
inline void append_u16_be(std::vector<uint8_t>& out, uint16_t v) {
  out.push_back(uint8_t((v >> 8) & 0xFF));
  out.push_back(uint8_t(v & 0xFF));
}

/**
 * @brief 读取 `read_u32_be` 对应数据。
 * @param p 字节指针。
 * @return 返回计算结果。
 */
inline uint32_t read_u32_be(const uint8_t* p) {
  return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | uint32_t(p[3]);
}

/**
 * @brief 读取 `read_u16_be` 对应数据。
 * @param p 字节指针。
 * @return 返回计算结果。
 */
inline uint16_t read_u16_be(const uint8_t* p) {
  return uint16_t((uint16_t(p[0]) << 8) | uint16_t(p[1]));
}

/**
 * @brief 执行 `dirname_from_path` 内部辅助逻辑。
 * @param path 路径。
 * @return 返回生成的字符串结果。
 */
inline std::string dirname_from_path(const std::string& path) {
  const std::size_t slash = path.find_last_of('/');
  if (slash == std::string::npos) return ".";
  if (slash == 0) return "/";
  return path.substr(0, slash);
}

/**
 * @brief 执行 `basename_from_path` 内部辅助逻辑。
 * @param path 路径。
 * @return 返回生成的字符串结果。
 */
inline std::string basename_from_path(const std::string& path) {
  const std::size_t slash = path.find_last_of('/');
  if (slash == std::string::npos) return path;
  return path.substr(slash + 1);
}

/**
 * @brief 执行 `fsync_directory` 内部辅助逻辑。
 * @param dir_path 目录路径。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool fsync_directory(const std::string& dir_path) {
  const int dir_fd =
      ::open(dir_path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (dir_fd < 0) return false;
  const bool ok = (::fsync(dir_fd) == 0);
  const int saved_errno = ok ? 0 : errno;
  ::close(dir_fd);
  if (!ok) errno = saved_errno;
  return ok;
}

/**
 * @brief 以断电一致的方式替换写文件。
 * @param path 目标文件路径。
 * @param data 待写入字节流。
 * @param size 字节数。
 * @return `true` 表示新版本已经原子替换为可见版本，`false` 表示写入失败。
 * @note WHY: 直接覆盖目标文件时，如果系统在写数据或回写元数据过程中掉电，磁盘上可能留下
 *       “旧文件已被截断，新文件又没写完”的中间态。这里固定采用
 *       `tmp + fdatasync(tmp) + rename + fsync(dir)`，
 *       让读取方只会看到“旧版本”或“完整新版本”，不会看到半写文件。
 */
inline bool WriteFileAtomically(const std::string& path, const uint8_t* data,
                                size_t size) {
  const std::string dir_path = dirname_from_path(path);
  const std::string base_name = basename_from_path(path);
  const std::string temp_template =
      dir_path + "/." + base_name + ".tmp.XXXXXX";
  std::vector<char> temp_path(temp_template.begin(), temp_template.end());
  temp_path.push_back('\0');

  int fd = ::mkstemp(temp_path.data());
  if (fd < 0) return false;

  if (::fchmod(fd, 0644) != 0) {
    const int saved_errno = errno;
    ::close(fd);
    ::unlink(temp_path.data());
    errno = saved_errno;
    return false;
  }

  size_t off = 0;
  while (off < size) {
    const ssize_t written = ::write(fd, data + off, size - off);
    if (written < 0) {
      if (errno == EINTR) continue;
      const int saved_errno = errno;
      ::close(fd);
      ::unlink(temp_path.data());
      errno = saved_errno;
      return false;
    }
    off += static_cast<size_t>(written);
  }

  if (::fdatasync(fd) != 0) {
    const int saved_errno = errno;
    ::close(fd);
    ::unlink(temp_path.data());
    errno = saved_errno;
    return false;
  }

  if (::close(fd) != 0) {
    const int saved_errno = errno;
    ::unlink(temp_path.data());
    errno = saved_errno;
    return false;
  }

  if (::rename(temp_path.data(), path.c_str()) != 0) {
    const int saved_errno = errno;
    ::unlink(temp_path.data());
    errno = saved_errno;
    return false;
  }

  if (!fsync_directory(dir_path)) {
    const int saved_errno = errno;
    ::unlink(temp_path.data());
    errno = saved_errno;
    return false;
  }

  return true;
}

/**
 * @brief 读取 `read_all_bytes` 对应数据。
 * @param path 路径。
 * @param out 输出对象。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool read_all_bytes(const std::string& path, std::vector<uint8_t>& out) {
  out.clear();
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return false;
  struct stat st{};
  if (::fstat(fd, &st) != 0) { ::close(fd); return false; }
  if (st.st_size <= 0 || st.st_size > 1024 * 1024) { ::close(fd); return false; }
  out.resize((size_t)st.st_size);
  size_t off = 0;
  while (off < out.size()) {
    const ssize_t r = ::read(fd, out.data() + off, out.size() - off);
    if (r < 0) { if (errno == EINTR) continue; ::close(fd); return false; }
    if (r == 0) break;
    off += (size_t)r;
  }
  ::close(fd);
  if (off != out.size()) return false;
  return true;
}



/**
 * @brief 写入 `write_meta_v1` 对应数据。
 * @param dir 目录路径。
 * @param m 事件元数据。
 * @param files 文件信息列表。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool write_meta_v1(const std::string& dir, const EventMetaV1& m, const std::vector<FileInfo25>& files) {
  std::vector<uint8_t> buf;
  buf.reserve(64 + files.size() * 25);

  buf.push_back('E'); buf.push_back('V'); buf.push_back('T'); buf.push_back('1');
  buf.push_back(m.ver);
  buf.push_back(m.rsv0);
  buf.push_back(m.event_type);
  buf.push_back(m.cur_pos);
  append_u32_be(buf, m.event_id);
  buf.insert(buf.end(), m.start_bcd, m.start_bcd + 8);
  buf.insert(buf.end(), m.end_bcd, m.end_bcd + 8);
  append_u16_be(buf, m.hydraulic);
  append_u16_be(buf, m.level);
  append_u32_be(buf, m.temp_humi);
  append_u32_be(buf, m.gap_value);
  buf.push_back(uint8_t(files.size() & 0xFF));
  buf.push_back(0); buf.push_back(0); buf.push_back(0);

  for (const auto& f : files) append_fileinfo25(buf, f);

  const uint32_t crc = crc32_ieee(buf.data(), buf.size());
  append_u32_be(buf, crc);

  std::string path = dir + "/" + kMetaName;
  return WriteFileAtomically(path, buf.data(), buf.size());
}

/**
 * @brief 读取 `read_meta_v1` 对应数据。
 * @param dir 目录路径。
 * @param m 事件元数据。
 * @param files 文件信息列表。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool read_meta_v1(const std::string& dir, EventMetaV1& m, std::vector<FileInfo25>& files) {
  files.clear();
  std::vector<uint8_t> buf;
  std::string path = dir + "/" + kMetaName;
  if (!read_all_bytes(path, buf)) return false;
  if (buf.size() < 44 + 4) return false;
  if (!(buf[0] == 'E' && buf[1] == 'V' && buf[2] == 'T' && buf[3] == '1')) return false;
  const uint32_t crc_expect = read_u32_be(buf.data() + buf.size() - 4);
  const uint32_t crc_calc = crc32_ieee(buf.data(), buf.size() - 4);
  if (crc_expect != crc_calc) return false;

  m.ver = buf[4];
  m.rsv0 = buf[5];
  m.event_type = buf[6];
  m.cur_pos = buf[7];
  m.event_id = read_u32_be(buf.data() + 8);
  std::memcpy(m.start_bcd, buf.data() + 12, 8);
  std::memcpy(m.end_bcd, buf.data() + 20, 8);
  m.hydraulic = read_u16_be(buf.data() + 28);
  m.level = read_u16_be(buf.data() + 30);
  m.temp_humi = read_u32_be(buf.data() + 32);
  m.gap_value = read_u32_be(buf.data() + 36);
  const uint8_t cnt = buf[40];

  const size_t need = 44 + size_t(cnt) * 25 + 4;
  if (buf.size() != need) return false;

  files.reserve(cnt);
  const uint8_t* p = buf.data() + 44;
  for (uint8_t i = 0; i < cnt; ++i) {
    FileInfo25 f;
    f.file_id = read_u32_be(p + 0);
    f.file_len = read_u32_be(p + 4);
    std::memcpy(f.start_bcd, p + 8, 8);
    f.flag = p[16];
    f.duration_ms = read_u32_be(p + 17);
    f.pos_mask = p[21];
    f.reserved = read_u16_be(p + 22);
    f.file_type = p[24];
    files.push_back(f);
    p += 25;
  }
  return true;
}

/**
 * @brief 判断 `is_hex8_dirname` 条件是否成立。
 * @param s 输入对象。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool is_hex8_dirname(const char* s) {
  if (!s) return false;
  if (std::strlen(s) != 8) return false;
  for (int i = 0; i < 8; ++i) {
    const char c = s[i];
    const bool ok = (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
    if (!ok) return false;
  }
  return true;
}

/**
 * @brief 解析 `parse_hex8_u32` 对应输入。
 * @param s 输入对象。
 * @return 返回计算结果。
 */
inline uint32_t parse_hex8_u32(const char* s) {
  uint32_t v = 0;
  for (int i = 0; i < 8; ++i) {
    const char c = s[i];
    uint8_t d = 0;
    if (c >= '0' && c <= '9') d = uint8_t(c - '0');
    else if (c >= 'A' && c <= 'F') d = uint8_t(c - 'A' + 10);
    else if (c >= 'a' && c <= 'f') d = uint8_t(c - 'a' + 10);
    v = (v << 4) | d;
  }
  return v;
}

/**
 * @brief 执行 `rm_rf` 内部辅助逻辑。
 * @param path 路径。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool rm_rf(const std::string& path) {
  struct stat st{};
  if (::lstat(path.c_str(), &st) != 0) return false;

  if (S_ISDIR(st.st_mode)) {
    DIR* d = ::opendir(path.c_str());
    if (!d) return false;
    struct dirent* ent = nullptr;
    while ((ent = ::readdir(d)) != nullptr) {
      if (!std::strcmp(ent->d_name, ".") || !std::strcmp(ent->d_name, "..")) continue;
      std::string child = path + "/" + ent->d_name;
      (void)rm_rf(child);
    }
    ::closedir(d);
    return (::rmdir(path.c_str()) == 0);
  }

  return (::unlink(path.c_str()) == 0);
}

struct EventListItem {
  uint32_t event_id = 0;
  uint8_t event_type = 2;
};

/**
 * @brief 枚举 `list_all_events` 对应集合。
 * @param type_filter 事件类型过滤值。
 * @param out 输出对象。
 */
inline void list_all_events(uint8_t type_filter, std::vector<EventListItem>& out) {
  out.clear();
  DIR* d = ::opendir(kEventsDir);
  if (!d) return;

  struct dirent* ent = nullptr;
  while ((ent = ::readdir(d)) != nullptr) {
    if (!is_hex8_dirname(ent->d_name)) continue;
    const uint32_t eid = parse_hex8_u32(ent->d_name);
    std::string dir = std::string(kEventsDir) + "/" + ent->d_name;

    EventMetaV1 m;
    std::vector<FileInfo25> files;
    if (!read_meta_v1(dir, m, files)) {
      // 兼容：没有 meta.bin 时默认事件类型=2（上位机事件）
      m.event_id = eid;
      m.event_type = 2;
      m.rsv0 = 0;
    }
    if (type_filter != 0xFF && m.event_type != type_filter) continue;

    EventListItem it;
    it.event_id = eid;
    it.event_type = m.event_type;
    out.push_back(it);
  }
  ::closedir(d);

  std::sort(out.begin(), out.end(), [](const EventListItem& a, const EventListItem& b){
    return a.event_id > b.event_id; // 最新在前
  });
}

/**
 * @brief 构建 `build_event_detail_payload` 对应结果。
 * @param event_id 事件 ID。
 * @param payload_out 输出负载缓冲区。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool build_event_detail_payload(uint32_t event_id, std::vector<uint8_t>& payload_out) {
  payload_out.clear();
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%s/%08X", kEventsDir, event_id);
  std::string dir = buf;

  EventMetaV1 m;
  std::vector<FileInfo25> files;

  if (!read_meta_v1(dir, m, files)) return false;

  payload_out.push_back(0x00); // result
  payload_out.push_back(0x00); // reason
  append_u32_be(payload_out, m.event_id);
  payload_out.push_back(m.event_type);
  payload_out.push_back(m.cur_pos);
  payload_out.insert(payload_out.end(), m.start_bcd, m.start_bcd + 8);
  payload_out.insert(payload_out.end(), m.end_bcd, m.end_bcd + 8);
  append_u16_be(payload_out, m.hydraulic);
  append_u16_be(payload_out, m.level);
  append_u32_be(payload_out, m.temp_humi);
  append_u32_be(payload_out, m.gap_value);

  payload_out.push_back(uint8_t(files.size() & 0xFF));
  for (const auto& f : files) append_fileinfo25(payload_out, f);
  return true;
}

inline btt::proto::Frame make_resp_2b(uint8_t cmd, uint16_t seq, uint8_t result, uint8_t reason);

/**
 * @brief 处理 `handle_event_list_3D_req` 对应流程。
 * @param fr 协议帧。
 * @param outbound 输出队列。
 */
inline void handle_event_list_3D_req(const btt::proto::Frame& fr,
                                     btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;
  uint8_t type_filter = 0xFF;

  if (fr.payload.size() != 1) {
    result = 0x01;
  } else {
    type_filter = fr.payload[0];
  }

  std::vector<EventListItem> items;
  if (result == 0x00) list_all_events(type_filter, items);

  // payload: result(1) reason(1) count(2) + N*(event_id(4)+event_type(1))
  btt::proto::Frame resp;
  resp.cmd = 0x3D;
  resp.level = 0x01;
  resp.seq = fr.seq;

  resp.payload.push_back(result);
  resp.payload.push_back(reason);

  const uint16_t cnt = (uint16_t)std::min<size_t>(items.size(), 200); // 防止超长
  append_u16_be(resp.payload, cnt);

  for (uint16_t i = 0; i < cnt; ++i) {
    append_u32_be(resp.payload, items[i].event_id);
    resp.payload.push_back(items[i].event_type);
  }

  LOGI("0x3D EVENT_LIST RESP: filter=0x%02X cnt=%u", type_filter, cnt);
  outbound.push(std::move(resp));
}

/**
 * @brief 处理 `handle_event_detail_3E_req` 对应流程。
 * @param fr 协议帧。
 * @param outbound 输出队列。
 */
inline void handle_event_detail_3E_req(const btt::proto::Frame& fr,
                                      btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  if (fr.payload.size() != 4) {
    result = 0x01;
  }

  btt::proto::Frame resp;
  resp.cmd = 0x3E;
  resp.level = 0x01;
  resp.seq = fr.seq;

  if (result != 0x00) {
    resp.payload = {result, reason};
    outbound.push(std::move(resp));
    return;
  }

  const uint32_t event_id = read_u32_be(fr.payload.data());

  std::vector<uint8_t> payload;
  if (!build_event_detail_payload(event_id, payload)) {
    resp.payload = {0x01, 0x01}; // not found / meta invalid
    outbound.push(std::move(resp));
    LOGW("0x3E EVENT_DETAIL failed: event_id=%08X", event_id);
    return;
  }

  resp.payload = std::move(payload);
  LOGI("0x3E EVENT_DETAIL RESP: event_id=%08X", event_id);
  outbound.push(std::move(resp));
}

/**
 * @brief 处理 `handle_event_clear_3F_req` 对应流程。
 * @param fr 协议帧。
 * @param outbound 输出队列。
 */
inline void handle_event_clear_3F_req(const btt::proto::Frame& fr,
                                     btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  if (fr.payload.size() != 4) {
    result = 0x01;
  }

  uint32_t event_id = 0;
  if (result == 0x00) event_id = read_u32_be(fr.payload.data());

  if (result == 0x00) {
    if (event_id == 0xFFFFFFFFu) {
      // clear all
      DIR* d = ::opendir(kEventsDir);
      if (d) {
        struct dirent* ent = nullptr;
        while ((ent = ::readdir(d)) != nullptr) {
          if (!is_hex8_dirname(ent->d_name)) continue;
          std::string dir = std::string(kEventsDir) + "/" + ent->d_name;
          (void)rm_rf(dir);
          const uint32_t eid = parse_hex8_u32(ent->d_name);
          cleanup_event_tmp_tar(eid);
        }
        ::closedir(d);
      }
      LOGI("0x3F CLEAR ALL events");
    } else {
      char buf[64];
      std::snprintf(buf, sizeof(buf), "%s/%08X", kEventsDir, event_id);
      std::string dir = buf;
      if (!rm_rf(dir)) {
        result = 0x01;
        reason = 0x01;
      }
      LOGI("0x3F CLEAR event_id=%08X result=%u", event_id, result);
      // 同步清理 /tmp 下对应的压缩包（如果存在）
      cleanup_event_tmp_tar(event_id);
    }
  }

  outbound.push(make_resp_2b(0x3F, fr.seq, result, reason));
}



// ---------- M6：事件文件流上传 0x31/0x32/0x33 + 0x50(媒体通道) ----------
// 约束：仅支持一路上传；压缩包落地 /tmp（tmpfs），掉电自动清除。
// 兼容模式：
// - 目录ID：tar 打包上传（旧流程）
// - 文件ID：原文件直传（新流程）
// 成功结束条件：发送最后一个 0x50(flag=2) -> 关闭媒体 socket

static constexpr size_t kEventUploadChunkBytes = 1024;   // 1KB
static constexpr int    kEventUploadMaxSeconds = 60;     // >60s 视为超时，发送 0x32


/**
 * @brief 构造 `make_event_dir_path` 对应结果。
 * @param event_id 事件 ID。
 * @return 返回生成的字符串结果。
 */
inline std::string make_event_dir_path(uint32_t event_id) {//拼接目标目录
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%s/%08X", kEventsDir, event_id);
  return std::string(buf);
}


/**
 * @brief 构造 `make_event_tar_path` 对应结果。
 * @param event_id 事件 ID。
 * @return 返回生成的字符串结果。
 */
inline std::string make_event_tar_path(uint32_t event_id) {
  char buf[64];
  std::snprintf(buf, sizeof(buf), "/tmp/evt_%08X.tar.gz", event_id);
  return std::string(buf);
}

/**
 * @brief 执行 `path_is_dir` 内部辅助逻辑。
 * @param p 字节指针。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool path_is_dir(const std::string& p) {//调用系统函数产看目录状态
  struct stat st{};
  if (::stat(p.c_str(), &st) != 0) return false;
  return S_ISDIR(st.st_mode);
}

/**
 * @brief 执行 `file_exists` 内部辅助逻辑。
 * @param p 字节指针。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool file_exists(const std::string& p) {
  return ::access(p.c_str(), F_OK) == 0;
}

/**
 * @brief 执行 `path_is_regular_file` 内部辅助逻辑。
 * @param p 字节指针。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool path_is_regular_file(const std::string& p) {
  struct stat st{};
  if (::stat(p.c_str(), &st) != 0) return false;
  return S_ISREG(st.st_mode);
}

/**
 * @brief 执行 `pick_existing_media_path` 内部辅助逻辑。
 * @param dir 目录路径。
 * @param file_id 文件 ID。
 * @param ext 文件扩展名。
 * @param out_path 输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool pick_existing_media_path(const std::string& dir, uint32_t file_id,
                                     const char* ext, std::string& out_path) {
  const std::string p_new = make_media_file_path_v2(dir, file_id, ext);
  if (path_is_regular_file(p_new)) { out_path = p_new; return true; }
  const std::string p_old = make_media_file_path_legacy(dir, file_id, ext);
  if (path_is_regular_file(p_old)) { out_path = p_old; return true; }
  return false;
}

/**
 * @brief 执行 `file_type_from_ext` 内部辅助逻辑。
 * @param ext 文件扩展名。
 * @return 返回计算结果。
 */
inline uint8_t file_type_from_ext(const char* ext) {
  if (std::strcmp(ext, "h264") == 0) return 0x01;
  return 0x00; // jpg/jpeg 统一按图片
}

/**
 * @brief 执行 `ext_from_file_type` 内部辅助逻辑。
 * @param file_type `file_type` 参数。
 * @return 返回生成的字符串结果。
 */
inline const char* ext_from_file_type(uint8_t file_type) {
  return ((file_type & 0x0F) == 0x01) ? "h264" : "jpg";
}

/**
 * @brief 解析 `resolve_raw_file_upload` 对应映射。
 * @param file_id 文件 ID。
 * @param out_event_id 输出参数。
 * @param out_file_type 输出参数。
 * @param out_path 输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool resolve_raw_file_upload(uint32_t file_id, uint32_t& out_event_id,
                                    uint8_t& out_file_type, std::string& out_path) {
  out_event_id = 0;
  out_file_type = 0xFF;
  out_path.clear();

  DIR* d = ::opendir(kEventsDir);
  if (!d) return false;

  struct dirent* ent = nullptr;
  while ((ent = ::readdir(d)) != nullptr) {
    if (!is_hex8_dirname(ent->d_name)) continue;
    const uint32_t eid = parse_hex8_u32(ent->d_name);
    const std::string dir = std::string(kEventsDir) + "/" + ent->d_name;

    EventMetaV1 m{};
    std::vector<FileInfo25> files;
    if (read_meta_v1(dir, m, files)) {
      for (const auto& fi : files) {
        if (fi.file_id != file_id) continue;

        std::string p;
        const char* ext = ext_from_file_type(fi.file_type);
        if (pick_existing_media_path(dir, file_id, ext, p) ||
            pick_existing_media_path(dir, file_id, "jpg", p) ||
            pick_existing_media_path(dir, file_id, "h264", p)) {
          out_event_id = eid;
          out_file_type = fi.file_type;
          out_path = p;
          ::closedir(d);
          return true;
        }
      }
    }

    // 兼容老数据：meta 丢失/不一致时，直接按文件名兜底
    std::string p;
    if (pick_existing_media_path(dir, file_id, "jpg", p)) {
      out_event_id = eid;
      out_file_type = file_type_from_ext("jpg");
      out_path = p;
      ::closedir(d);
      return true;
    }
    if (pick_existing_media_path(dir, file_id, "h264", p)) {
      out_event_id = eid;
      out_file_type = file_type_from_ext("h264");
      out_path = p;
      ::closedir(d);
      return true;
    }
  }

  ::closedir(d);
  return false;
}

enum class EventUploadKind : uint8_t {
  TarByEventId = 0,
  RawByFileId = 1,
};

/**
 * @brief 执行 `event_upload_kind_name` 内部辅助逻辑。
 * @param k 事件上传类型。
 * @return 返回生成的字符串结果。
 */
inline const char* event_upload_kind_name(EventUploadKind k) {
  return (k == EventUploadKind::TarByEventId) ? "tar" : "raw";
}

/**
 * @brief 执行 `file_size_bytes` 内部辅助逻辑。
 * @param p 字节指针。
 * @return 返回计算结果。
 */
inline uint64_t file_size_bytes(const std::string& p) {//st_size (文件大小)、st_mode (文件类型和权限)、st_mtime (最后修改时间)。
  struct stat st{};
  if (::stat(p.c_str(), &st) != 0) return 0;
  return (uint64_t)st.st_size;
}

/**
 * @brief 执行 `unlink_noerr` 内部辅助逻辑。
 * @param p 字节指针。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool unlink_noerr(const std::string& p) {
  if (::unlink(p.c_str()) == 0) return true;
  if (errno == ENOENT) return true;
  return false;
}

/**
 * @brief 执行 `run_cmd_wait` 内部辅助逻辑。
 * @param cmd 协议命令字。
 * @return 返回计算结果。
 */
inline int run_cmd_wait(const std::string& cmd) {
  int rc = ::system(cmd.c_str());
  if (rc == -1) return -1;
#ifdef WIFEXITED
  if (WIFEXITED(rc)) return WEXITSTATUS(rc);
#endif
  return rc;
}

// 生成 /tmp/evt_xxxxxxxx.tar.gz （压缩整个事件目录）
/**
 * @brief 构建 `build_event_tar_gz` 对应结果。
 * @param event_id 事件 ID。
 * @param out_tar_path 输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
inline bool build_event_tar_gz(uint32_t event_id, std::string& out_tar_path) {
  out_tar_path = make_event_tar_path(event_id);//拼接生成的事件包名字/目录

  // 事件目录名固定 8HEX（与 /data/events/<8HEX> 一致）
  char name8[16];
  std::snprintf(name8, sizeof(name8), "%08X", event_id);

  // 先删旧包（避免续传/重试时内容不一致）
  (void)unlink_noerr(out_tar_path);

  // tar -czf /tmp/evt_xxxxxxxx.tar.gz -C /data/events xxxxxxxx
  std::string cmd;
  cmd.reserve(256);
  cmd += "tar -czf ";
  cmd += out_tar_path;
  cmd += " -C ";
  cmd += kEventsDir;
  cmd += " ";
  cmd += name8;

  int ec = run_cmd_wait(cmd);
  if (ec != 0) {
    LOGW("build_event_tar_gz failed: ec=%d cmd=%s", ec, cmd.c_str());
    (void)unlink_noerr(out_tar_path);
    return false;
  }

  const uint64_t sz = file_size_bytes(out_tar_path);
  if (sz == 0) {
    LOGW("build_event_tar_gz got empty file: %s", out_tar_path.c_str());
    (void)unlink_noerr(out_tar_path);
    return false;
  }
  return true;
}

// 清理单个事件对应的临时压缩包（供 0x3F/上传结束使用）
/**
 * @brief 清理 `cleanup_event_tmp_tar` 相关资源。
 * @param event_id 事件 ID。
 */
inline void cleanup_event_tmp_tar(uint32_t event_id) {
  const std::string tar = make_event_tar_path(event_id);
  (void)unlink_noerr(tar);
}

/**
 * @brief 构造 `make_event_upload_32` 对应结果。
 * @param key_id 上传键值 ID。
 * @param event_id 事件 ID。
 * @param end_reason 结束原因码。
 * @param rt 共享运行态。
 * @return 返回构造后的协议帧。
 */
inline btt::proto::Frame make_event_upload_32(uint32_t key_id, uint32_t event_id, uint8_t end_reason, RuntimeConfig& rt) {
  btt::proto::Frame f;
  f.cmd = 0x32;
  f.level = 0x00; // 设备->上位机 请求
  f.seq = rt.seq.fetch_add(1);

  f.payload.reserve(9);
  append_u32_be(f.payload, key_id);
  append_u32_be(f.payload, event_id);
  f.payload.push_back(end_reason);
  return f;
}

struct EventUploadReq31 {
  uint32_t key_id = 0;
  uint32_t event_id = 0; // 请求中的 ID：兼容下可能是目录ID，也可能是文件ID
  uint32_t offset = 0;
  uint32_t ip_be = 0;
  uint16_t port = 0;

  EventUploadKind kind = EventUploadKind::TarByEventId;
  uint32_t owner_event_id = 0; // 原始文件所属事件（仅 RawByFileId 有意义）
  uint32_t tx_file_id = 0;     // 0x50 里传输的 file_id
  uint8_t tx_file_type = 0xFF; // 0x50 里传输的 file_type
  std::string source_path;     // RawByFileId 直接上传该路径；tar 模式由线程内构建
};

// 单路事件上传工作器（一个实例对应一路并发上传）
class EventUploadWorker {
public:
  struct BusySnapshot {
    uint32_t key_id = 0;
    uint32_t req_id = 0;
    uint32_t owner_event_id = 0;
    uint32_t tx_file_id = 0;
    uint16_t port = 0;
    EventUploadKind kind = EventUploadKind::TarByEventId;
    int64_t start_ms = 0;
    uint64_t sent_bytes = 0;
    uint64_t total_bytes = 0;
  };

  /**
   * @brief 构造 `EventUploadWorker` 对象并初始化默认状态。
   */
  EventUploadWorker() = default;//默认，不初始
  /**
   * @brief 析构 `EventUploadWorker` 对象并释放关联资源。
   */
  ~EventUploadWorker() { stop_and_join(); }//结束

  /**
   * @brief 查询当前任务是否忙碌。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool busy() const { return active_.load(); }//私有变量

  /**
   * @brief 获取 `get_busy_snapshot` 对应信息。
   * @param out 输出对象。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool get_busy_snapshot(BusySnapshot& out) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!active_.load()) return false;
    out.key_id = cur_.key_id;
    out.req_id = cur_.event_id;
    out.owner_event_id = cur_.owner_event_id;
    out.tx_file_id = cur_.tx_file_id;
    out.port = cur_.port;
    out.kind = cur_.kind;
    out.start_ms = active_start_ms_.load();
    out.sent_bytes = sent_bytes_.load();
    out.total_bytes = total_bytes_.load();
    return true;
  }

  /**
   * @brief 执行 `start` 内部辅助逻辑。
   * @param r 上传请求。
   * @param rt 共享运行态。
   * @param ctrl_out 控制输出队列。
   * @param stop_flag 停止标志。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool start(const EventUploadReq31& r, RuntimeConfig& rt,
             btt::utils::BlockingQueue<btt::proto::Frame>& ctrl_out,
             std::atomic<bool>& stop_flag) {
    std::lock_guard<std::mutex> lk(mu_);
    if (active_.load()) return false;

    // 回收旧线程
    if (th_.joinable()) th_.join();

    cur_ = r;
    cancel_.store(false);
    cancel_reason_.store(0);
    media_fd_.store(-1);
    active_start_ms_.store(now_ms_steady());
    sent_bytes_.store(0);
    total_bytes_.store(0);
    active_.store(true);

    th_ = std::thread([this, r, &rt, &ctrl_out, &stop_flag]{
      this->thread_main(r, rt, ctrl_out, stop_flag);
    });
    return true;
  }

  // 由 0x33 调用：请求终止
  /**
   * @brief 请求执行 `request_cancel` 操作。
   * @param key_id 上传键值 ID。
   * @param event_id 事件 ID。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool request_cancel(uint32_t key_id, uint32_t event_id) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!active_.load()) return false;
    if (cur_.key_id != key_id || cur_.event_id != event_id) return false;
    LOGW("M6 upload: cancel by host key=%08X req_id=%08X active_key=%08X active_req=%08X",
         key_id, event_id, cur_.key_id, cur_.event_id);
    cancel_reason_.store(2); // 上位机终止
    cancel_.store(true);
    const int fd = media_fd_.load();
    if (fd >= 0) ::shutdown(fd, SHUT_RDWR); // 让发送尽快退出
    return true;
  }

  // M8 抢占：指定结束原因码（0x30/0x50/0x51 等），并尽快打断媒体通道
  /**
   * @brief 请求执行 `request_preempt` 操作。
   * @param end_reason 结束原因码。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool request_preempt(uint8_t end_reason) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!active_.load()) return false;
    LOGW("M6 upload: preempt end_reason=0x%02X active_key=%08X active_req=%08X",
         end_reason, cur_.key_id, cur_.event_id);
    cancel_reason_.store(end_reason);
    cancel_.store(true);
    const int fd = media_fd_.load();
    if (fd >= 0) ::shutdown(fd, SHUT_RDWR);
    return true;
  }

  /**
   * @brief 停止 `stop_and_join` 对应流程。
   */
  void stop_and_join() {
    LOGW("M6 upload: stop_and_join requested");
    cancel_reason_.store(2);
    cancel_.store(true);
    {
      std::lock_guard<std::mutex> lk(mu_);
      // nothing
      (void)lk;
    }
    if (th_.joinable()) th_.join();
    active_.store(false);
  }

private:
  /**
   * @brief 执行 `finish` 内部辅助逻辑。
   */
  void finish() {
    std::lock_guard<std::mutex> lk(mu_);
    active_.store(false);
  }

  /**
   * @brief 执行 `thread_main` 内部辅助逻辑。
   * @param r 上传请求。
   * @param rt 共享运行态。
   * @param ctrl_out 控制输出队列。
   * @param stop_flag 停止标志。
   */
  void thread_main(const EventUploadReq31& r, RuntimeConfig& rt,
                   btt::utils::BlockingQueue<btt::proto::Frame>& ctrl_out,
                   std::atomic<bool>& stop_flag) {
    const int64_t t0 = now_ms_steady();

    // 结束原因定义（与上位机未必完全一致，按需扩展）
    // 0:正常结束 1:超时 2:上位机终止 3:内部错误/IO 4:连接失败 5:压缩失败 6:参数错误
    uint8_t end_reason = 3;

    std::string src_path;
    bool need_cleanup_tmp_tar = false;
    if (r.kind == EventUploadKind::TarByEventId) {
      const std::string event_dir = make_event_dir_path(r.owner_event_id);
      if (!path_is_dir(event_dir)) {
        LOGW("M6 upload: event dir not found: %s", event_dir.c_str());
        end_reason = 6;
        ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
        finish();
        return;
      }

      std::string tar_path = make_event_tar_path(r.owner_event_id);
      bool need_build = (r.offset == 0) || !file_exists(tar_path);
      if (need_build) {
        if (!build_event_tar_gz(r.owner_event_id, tar_path)) {
          end_reason = 5;
          ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
          finish();
          return;
        }
      }
      src_path = tar_path;
      need_cleanup_tmp_tar = true;
    } else {
      src_path = r.source_path;
      if (!path_is_regular_file(src_path)) {
        LOGW("M6 upload: raw file not found: %s", src_path.c_str());
        end_reason = 6;
        ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
        finish();
        return;
      }
    }

    const uint64_t total = file_size_bytes(src_path);
    total_bytes_.store(total);
    if (total == 0 || (uint64_t)r.offset > total) {
      LOGW("M6 upload: bad offset=%u total=%llu src=%s", r.offset, (unsigned long long)total, src_path.c_str());
      end_reason = 6;
      ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
      finish();
      return;
    }

    const uint32_t tx_file_id = r.tx_file_id ? r.tx_file_id : r.event_id;
    const uint8_t tx_file_type = r.tx_file_type;
    LOGI("M6 upload: start key=%08X req_id=%08X kind=%s owner_event=%08X tx_file=%08X type=0x%02X offset=%u total=%llu src=%s",
         r.key_id, r.event_id, event_upload_kind_name(r.kind), r.owner_event_id, tx_file_id, tx_file_type,
         r.offset, (unsigned long long)total, src_path.c_str());

    // 2) 打开媒体通道（TCP client 连接上位机指定 IP:Port）
    btt::net::TcpClient media;
    const std::string ip = ip_be_to_string(r.ip_be);
    if (!media.connect_to(ip, r.port, 3000)) {
      LOGW("M6 upload: connect media failed: %s:%u", ip.c_str(), r.port);
      end_reason = 4;
      ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
      finish();
      return;
    }
    media_fd_.store(media.fd());
    LOGI("M6 upload: media connected to %s:%u mode=%u req_id=%08X tx_file=%08X src=%s",
         ip.c_str(), r.port, uint8_t(r.kind), r.event_id, tx_file_id, src_path.c_str());

    // 3) 发送 0x50 文件流
    int fd = ::open(src_path.c_str(), O_RDONLY);
    if (fd < 0) {
      LOGW("M6 upload: open source failed: %s errno=%d", src_path.c_str(), errno);
      end_reason = 3;
      media.close_fd();
      ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
      finish();
      return;
    }

    if (::lseek(fd, (off_t)r.offset, SEEK_SET) < 0) {//lseek函数用于移动文件读写指针到指定位置，偏移
      LOGW("M6 upload: lseek failed offset=%u errno=%d", r.offset, errno);
      end_reason = 6;
      ::close(fd);
      media.close_fd();
      ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
      finish();
      return;
    }

    uint16_t stream_seq = (uint16_t)(r.offset / kEventUploadChunkBytes);
    uint16_t frame_seq = 1;

    uint64_t pos = r.offset;
    bool first = true;
    bool ok = false;

    std::vector<uint8_t> chunk;
    chunk.resize(kEventUploadChunkBytes);

    while (!stop_flag.load()) {
      if (cancel_.load()) {
        uint8_t cr = cancel_reason_.load();
        end_reason = cr ? cr : 2;
        LOGW("M6 upload: canceled mid-transfer key=%08X req_id=%08X end_reason=0x%02X sent=%llu/%llu",
             r.key_id, r.event_id, end_reason,
             (unsigned long long)(pos - r.offset), (unsigned long long)(total - r.offset));
        break;
      }

      const int64_t now = now_ms_steady();
      if (now - t0 > (int64_t)kEventUploadMaxSeconds * 1000) {
        end_reason = 1;
        LOGW("M6 upload: timeout key=%08X req_id=%08X elapsed_ms=%lld sent=%llu/%llu",
             r.key_id, r.event_id, (long long)(now - t0),
             (unsigned long long)(pos - r.offset), (unsigned long long)(total - r.offset));
        break;
      }

      ssize_t n = ::read(fd, chunk.data(), kEventUploadChunkBytes);
      if (n < 0) {
        if (errno == EINTR) continue;
        LOGW("M6 upload: read failed errno=%d", errno);
        end_reason = 3;
        break;
      }

      // EOF：理论上不应出现（total>0），但兜底：发一个空的 flag=2 结束包
      if (n == 0) {
        btt::proto::Frame f;
        f.cmd = 0x50;
        f.level = 0x00;
        f.seq = frame_seq++;

        f.payload.reserve(12);
        append_u32_be(f.payload, r.key_id);
        append_u32_be(f.payload, tx_file_id);
        f.payload.push_back(tx_file_type);
        f.payload.push_back(2); // end
        append_u16_be(f.payload, stream_seq);

        auto bytes = btt::proto::encode(f);
        if (bytes.empty() || !media.send_all(bytes.data(), bytes.size())) {
          LOGW("M6 upload: send final empty 0x50 failed at seq=%u", stream_seq);
          end_reason = cancel_.load() ? (cancel_reason_.load() ? cancel_reason_.load() : 2) : 3;
          break;
        }

        LOGI("M6 upload: final 0x50 sent key=%08X req_id=%08X seq=%u flag=2 bytes=0",
             r.key_id, r.event_id, stream_seq);
        ok = true;
        end_reason = 0;
        break;
      }

      pos += (uint64_t)n;
      sent_bytes_.store(pos - r.offset);
      const bool is_last = (pos >= total);
      const uint16_t tx_stream_seq = stream_seq;

      uint8_t flag = 1;
      if (is_last) flag = 2;
      else if (first && r.offset == 0) flag = 0;
      else flag = 1;
      first = false;

      btt::proto::Frame f;
      f.cmd = 0x50;
      f.level = 0x00;
      f.seq = frame_seq++;

      f.payload.reserve(12 + (size_t)n);
      append_u32_be(f.payload, r.key_id);
      append_u32_be(f.payload, tx_file_id);
      f.payload.push_back(tx_file_type);
      f.payload.push_back(flag);            // 0/1/2
      append_u16_be(f.payload, stream_seq);
      f.payload.insert(f.payload.end(), chunk.begin(), chunk.begin() + n);

      auto bytes = btt::proto::encode(f);
      if (bytes.empty() || !media.send_all(bytes.data(), bytes.size())) {
        LOGW("M6 upload: send failed at seq=%u flag=%u", stream_seq, flag);
        end_reason = cancel_.load() ? (cancel_reason_.load() ? cancel_reason_.load() : 2) : 3;
        break;
      }

      stream_seq++;

      if (is_last) {
        LOGI("M6 upload: final 0x50 sent key=%08X req_id=%08X seq=%u flag=2 bytes=%zd",
             r.key_id, r.event_id, tx_stream_seq, n);
        ok = true;
        end_reason = 0;
        break;
      }
    }

    ::close(fd);
    media.close_fd();
    media_fd_.store(-1);

    // 成功：删除 /tmp 下的 tar.gz
    if (need_cleanup_tmp_tar && (ok || end_reason == 0x30 || end_reason == 0x50 || end_reason == 0x51)) {
      cleanup_event_tmp_tar(r.owner_event_id);
    }

    const int64_t cost_ms = now_ms_steady() - t0;
    LOGI("M6 upload: finish key=%08X req_id=%08X kind=%s end_reason=0x%02X ok=%u sent=%llu/%llu cost_ms=%lld",
         r.key_id, r.event_id, event_upload_kind_name(r.kind), end_reason, ok ? 1 : 0,
         (unsigned long long)sent_bytes_.load(), (unsigned long long)total_bytes_.load(),
         (long long)cost_ms);
    if (!ok || end_reason != 0) {
      ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
    }
    finish();
  }

  std::mutex mu_;
  std::thread th_;
  std::atomic<bool> active_{false};
  std::atomic<bool> cancel_{false};
  std::atomic<uint8_t> cancel_reason_{0};
  std::atomic<int> media_fd_{-1};
  std::atomic<int64_t> active_start_ms_{0};
  std::atomic<uint64_t> sent_bytes_{0};
  std::atomic<uint64_t> total_bytes_{0};
  EventUploadReq31 cur_{};
};

// 多路上传池：每个槽位独立建立媒体连接，可并发上传多个文件
class EventUploader {
public:
  using BusySnapshot = EventUploadWorker::BusySnapshot;

  /**
   * @brief 查询当前任务是否忙碌。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool busy() const {
    return active_count() >= kParallelSlots;
  }

  /**
   * @brief 获取当前活动任务数量。
   * @return 返回计算结果。
   */
  size_t active_count() const {
    size_t n = 0;
    for (const auto& w : workers_) if (w.busy()) ++n;
    return n;
  }

  /**
   * @brief 获取并行槽位容量。
   * @return 返回计算结果。
   */
  size_t capacity() const { return kParallelSlots; }

  /**
   * @brief 获取 `get_busy_snapshot` 对应信息。
   * @param out 输出对象。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool get_busy_snapshot(BusySnapshot& out) {
    for (auto& w : workers_) {
      if (w.get_busy_snapshot(out)) return true;
    }
    return false;
  }

  /**
   * @brief 执行 `start` 内部辅助逻辑。
   * @param r 上传请求。
   * @param rt 共享运行态。
   * @param ctrl_out 控制输出队列。
   * @param stop_flag 停止标志。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool start(const EventUploadReq31& r, RuntimeConfig& rt,
             btt::utils::BlockingQueue<btt::proto::Frame>& ctrl_out,
             std::atomic<bool>& stop_flag) {
    for (size_t i = 0; i < workers_.size(); ++i) {
      if (workers_[i].start(r, rt, ctrl_out, stop_flag)) {
        LOGI("M6 pool: assigned slot=%zu key=%08X req_id=%08X active=%zu/%zu",
             i, r.key_id, r.event_id, active_count(), capacity());
        return true;
      }
    }
    return false;
  }

  /**
   * @brief 请求执行 `request_cancel` 操作。
   * @param key_id 上传键值 ID。
   * @param event_id 事件 ID。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool request_cancel(uint32_t key_id, uint32_t event_id) {
    bool any = false;
    for (auto& w : workers_) {
      any = w.request_cancel(key_id, event_id) || any;
    }
    return any;
  }

  /**
   * @brief 请求执行 `request_preempt` 操作。
   * @param end_reason 结束原因码。
   * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
   */
  bool request_preempt(uint8_t end_reason) {
    bool any = false;
    for (auto& w : workers_) {
      any = w.request_preempt(end_reason) || any;
    }
    return any;
  }

  /**
   * @brief 停止 `stop_and_join` 对应流程。
   */
  void stop_and_join() {
    for (auto& w : workers_) w.stop_and_join();
  }

private:
  static constexpr size_t kParallelSlots = 4;
  std::array<EventUploadWorker, kParallelSlots> workers_{};
};

// 0x31 事件上传命令（上位机->设备）
/**
 * @brief 处理 `handle_event_upload_31_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param up `up` 参数。
 * @param outbound 输出队列。
 * @param stop_flag 停止标志。
 */
inline void handle_event_upload_31_req(const btt::proto::Frame& fr, RuntimeConfig& rt, EventUploader& up,
                                      btt::utils::BlockingQueue<btt::proto::Frame>& outbound,
                                      std::atomic<bool>& stop_flag) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  EventUploadReq31 r{};
  if (fr.payload.size() != 18) {
    result = 0x01; reason = 0x01; // 参数错误
    LOGW("0x31 EVENT_UPLOAD bad payload len=%zu", fr.payload.size());
  } else {
    const uint8_t* p = fr.payload.data();
    r.key_id  = read_u32_be(p + 0);
    r.event_id = read_u32_be(p + 4);
    r.offset  = read_u32_be(p + 8);
    r.ip_be   = read_u32_be(p + 12);
    r.port    = read_u16_be(p + 16);
    LOGI("0x31 EVENT_UPLOAD req: key=%08X req_id=%08X offset=%u to %s:%u",
         r.key_id, r.event_id, r.offset, ip_be_to_string(r.ip_be).c_str(), r.port);

    if (r.port == 0) {
      result = 0x01; reason = 0x01;
      LOGW("0x31 EVENT_UPLOAD reject: key=%08X req_id=%08X reason=bad_port", r.key_id, r.event_id);
    } else if (up.busy()) {
      result = 0x01; reason = 0x02; // busy
      LOGW("0x31 EVENT_UPLOAD busy: key=%08X req_id=%08X active=%zu/%zu",
           r.key_id, r.event_id, up.active_count(), up.capacity());
      EventUploader::BusySnapshot bs{};
      if (up.get_busy_snapshot(bs)) {
        const int64_t busy_ms = now_ms_steady() - bs.start_ms;
        LOGW("0x31 EVENT_UPLOAD reject: key=%08X req_id=%08X reason=busy active{key=%08X req=%08X kind=%s owner_event=%08X tx_file=%08X sent=%llu/%llu busy_ms=%lld}",
             r.key_id, r.event_id, bs.key_id, bs.req_id, event_upload_kind_name(bs.kind),
             bs.owner_event_id, bs.tx_file_id,
             (unsigned long long)bs.sent_bytes, (unsigned long long)bs.total_bytes,
             (long long)busy_ms);
      } else {
        LOGW("0x31 EVENT_UPLOAD reject: key=%08X req_id=%08X reason=busy(active cleared during snapshot)",
             r.key_id, r.event_id);
      }
    } else {
      // 兼容逻辑：
      // 1) 新规则优先：按 ID 号段判定（0x1=事件、0x2=文件）
      // 2) 兜底：另一种模式再试一次（兼容旧数据）
      auto try_resolve_tar = [&]() -> bool {
        const std::string event_dir = make_event_dir_path(r.event_id);
        if (!path_is_dir(event_dir)) return false;
        r.kind = EventUploadKind::TarByEventId;
        r.owner_event_id = r.event_id;
        r.tx_file_id = r.event_id; // tar 模式沿用旧协议：file_id=event_id
        r.tx_file_type = 0xFF;
        r.source_path.clear();
        return true;
      };
      auto try_resolve_raw = [&]() -> bool {
        uint32_t owner_event_id = 0;
        uint8_t file_type = 0xFF;
        std::string file_path;
        if (!resolve_raw_file_upload(r.event_id, owner_event_id, file_type, file_path)) return false;
        r.kind = EventUploadKind::RawByFileId;
        r.owner_event_id = owner_event_id;
        r.tx_file_id = r.event_id;
        r.tx_file_type = file_type;
        r.source_path = file_path;
        return true;
      };

      bool resolved = false;
      const uint32_t id_tag = (r.event_id & 0xF0000000u);
      if (id_tag == kEventIdTag) {
        resolved = try_resolve_tar() || try_resolve_raw();
      } else if (id_tag == kFileIdTag) {
        resolved = try_resolve_raw() || try_resolve_tar();
      } else {
        // 旧数据无号段约束时，优先保持老行为（目录 tar）
        resolved = try_resolve_tar() || try_resolve_raw();
      }

      if (!resolved) {
        result = 0x01; reason = 0x03; // not found
        LOGW("0x31 EVENT_UPLOAD reject: key=%08X req_id=%08X reason=not_found id_tag=0x%X",
             r.key_id, r.event_id, (unsigned)((r.event_id >> 28) & 0xF));
      }

      if (result == 0x00) {
        // 启动上传线程
        if (!up.start(r, rt, outbound, stop_flag)) {
          result = 0x01; reason = 0x02;
          LOGW("0x31 EVENT_UPLOAD reject: key=%08X req_id=%08X reason=busy(race after resolve)", r.key_id, r.event_id);
        } else {
          LOGI("0x31 EVENT_UPLOAD start: key=%08X req_id=%08X offset=%u kind=%u owner_event=%08X to %s:%u",
               r.key_id, r.event_id, r.offset, uint8_t(r.kind), r.owner_event_id, ip_be_to_string(r.ip_be).c_str(), r.port);
        }
      }
    }
  }

  // 应答：result(1) reason(1) key(4) event(4)
  btt::proto::Frame resp;
  resp.cmd = 0x31;
  resp.level = 0x01;
  resp.seq = fr.seq;
  resp.payload.reserve(10);
  resp.payload.push_back(result);
  resp.payload.push_back(reason);
  append_u32_be(resp.payload, r.key_id);
  append_u32_be(resp.payload, r.event_id);

  outbound.push(std::move(resp));
}

// 0x33 终止事件上传命令（上位机->设备）
/**
 * @brief 处理 `handle_event_upload_33_req` 对应流程。
 * @param fr 协议帧。
 * @param up `up` 参数。
 * @param outbound 输出队列。
 */
inline void handle_event_upload_33_req(const btt::proto::Frame& fr, EventUploader& up,
                                      btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  uint32_t key_id = 0;
  uint32_t event_id = 0;

  if (fr.payload.size() != 8) {
    result = 0x01; reason = 0x01;
  } else {
    key_id = read_u32_be(fr.payload.data() + 0);
    event_id = read_u32_be(fr.payload.data() + 4);

    if (!up.request_cancel(key_id, event_id)) {
      result = 0x01; reason = 0x02; // not active / mismatch
    } else {
      LOGI("0x33 EVENT_UPLOAD abort: key=%08X event=%08X", key_id, event_id);
    }
  }

  btt::proto::Frame resp;
  resp.cmd = 0x33;
  resp.level = 0x01;
  resp.seq = fr.seq;
  resp.payload.reserve(10);
  resp.payload.push_back(result);
  resp.payload.push_back(reason);
  append_u32_be(resp.payload, key_id);
  append_u32_be(resp.payload, event_id);

  outbound.push(std::move(resp));
}

// ---------------- M8：触发方式/事件策略 + 操岔/过车流程 ----------------
static constexpr uint8_t kCmdInternalM8Timeout = 0xFE; // 内部超时触发（不走线协议）

/**
 * @brief M8 当前活动事件的单写者状态。
 * @note WHY: 该结构只允许 actor 线程写入，定时线程只依赖原子镜像判断是否注入超时事件。
 *       这样可以把“人工结束”“被抢占结束”“内部超时结束”三类转移收敛到单一串行执行点，
 *       避免同一事件被重复停止、重复补图或重复上报。
 */
struct M8ActiveEvent {
  bool active = false;
  uint8_t event_type = 0xFF; // 0=操岔 1=过车
  uint32_t event_id = 0;
  std::string dir;
  uint8_t start_bcd[8]{};
  int64_t start_ms_steady = 0;
};

/**
 * @brief 执行 `m8_get_effective_params` 相关内部逻辑。
 * @param rt 共享运行态。
 * @param event_type 事件类型。
 * @param timeout_s 时间长度，单位秒。
 * @param v_start_delay_ms 时间长度，单位毫秒。
 * @param v_stop_delay_ms 时间长度，单位毫秒。
 * @param v_max_dur_s 时间长度，单位秒。
 * @param pic_delay_ms 时间长度，单位毫秒。
 */
inline void m8_get_effective_params(RuntimeConfig& rt, uint8_t event_type,
                                    uint16_t& timeout_s,
                                    uint16_t& v_start_delay_ms,
                                    uint16_t& v_stop_delay_ms,
                                    uint16_t& v_max_dur_s,
                                    uint16_t& pic_delay_ms) {
  if (event_type == 0) {
    timeout_s        = rt.m8_timeout_s0.load();
    v_start_delay_ms = rt.m8_v_start_delay_ms0.load();
    v_stop_delay_ms  = rt.m8_v_stop_delay_ms0.load();
    v_max_dur_s      = rt.m8_v_max_dur_s0.load();
    pic_delay_ms     = rt.m8_pic_delay_ms0.load();
  } else {
    timeout_s        = rt.m8_timeout_s1.load();
    v_start_delay_ms = rt.m8_v_start_delay_ms1.load();
    v_stop_delay_ms  = rt.m8_v_stop_delay_ms1.load();
    v_max_dur_s      = rt.m8_v_max_dur_s1.load();
    pic_delay_ms     = rt.m8_pic_delay_ms1.load();
  }
}

/**
 * @brief 构造 `make_m8_pass_event_52` 对应结果。
 * @param rt 共享运行态。
 * @param action `action` 参数。
 * @param event_id 事件 ID。
 * @param start_bcd `start_bcd` 参数。
 * @param files 文件信息列表。
 * @return 返回构造后的协议帧。
 */
inline btt::proto::Frame make_m8_pass_event_52(RuntimeConfig& rt,
                                               uint8_t action,
                                               uint32_t event_id,
                                               const uint8_t start_bcd[8],
                                               const std::vector<FileInfo25>& files) {
  btt::proto::Frame f;
  f.cmd = 0x52;
  f.level = 0x00; // 设备->上位机 上报(请求)
  f.seq = rt.seq.fetch_add(1);

  const bool with_files = (action == 2 || action == 3);
  const uint8_t file_cnt = with_files ? uint8_t(files.size() & 0xFF) : 0;

  f.payload.reserve(15 + (with_files ? size_t(file_cnt) * 25 : 0));
  f.payload.push_back(action);
  append_u32_be(f.payload, event_id);
  f.payload.push_back(0x00); // 当前位置：00 不支持
  f.payload.insert(f.payload.end(), start_bcd, start_bcd + 8);
  f.payload.push_back(file_cnt);
  if (with_files) {
    for (const auto& fi : files) append_fileinfo25(f.payload, fi);
  }
  return f;
}

/**
 * @brief 构造 `make_m8_switch_event_53` 对应结果。
 * @param rt 共享运行态。
 * @param action `action` 参数。
 * @param event_id 事件 ID。
 * @param start_bcd `start_bcd` 参数。
 * @param files 文件信息列表。
 * @return 返回构造后的协议帧。
 */
inline btt::proto::Frame make_m8_switch_event_53(RuntimeConfig& rt,
                                                 uint8_t action,
                                                 uint32_t event_id,
                                                 const uint8_t start_bcd[8],
                                                 const std::vector<FileInfo25>& files) {
  btt::proto::Frame f;
  f.cmd = 0x53;
  f.level = 0x00; // 设备->上位机 上报(请求)
  f.seq = rt.seq.fetch_add(1);

  const bool with_files = (action == 2 || action == 3);
  const uint8_t file_cnt = with_files ? uint8_t(files.size() & 0xFF) : 0;

  f.payload.reserve(22 + (with_files ? size_t(file_cnt) * 25 : 0));
  f.payload.push_back(action);
  f.payload.push_back(0x00); // 扳动方向：本版本不支持
  append_u32_be(f.payload, event_id);
  f.payload.insert(f.payload.end(), start_bcd, start_bcd + 8);
  f.payload.push_back(0x00);                // 缺口值：不支持
  append_u32_be(f.payload, 0x00000000u);    // 温湿度：不支持
  append_u16_be(f.payload, 0x0000);         // 油位：不支持
  f.payload.push_back(file_cnt);
  if (with_files) {
    for (const auto& fi : files) append_fileinfo25(f.payload, fi);
  }
  return f;
}

/**
 * @brief 处理 `handle_m8_event_52_resp` 对应流程。
 * @param fr 协议帧。
 */
inline void handle_m8_event_52_resp(const btt::proto::Frame& fr) {
  if (fr.payload.size() < 2) return;
  const uint8_t result = fr.payload[0];
  const uint8_t reason = fr.payload[1];
  if (result == 0x00) LOGI("0x52 ACK: seq=%u result=%u reason=0x%02X", fr.seq, result, reason);
  else                LOGW("0x52 ACK FAIL: seq=%u result=%u reason=0x%02X", fr.seq, result, reason);
}

/**
 * @brief 处理 `handle_m8_event_53_resp` 对应流程。
 * @param fr 协议帧。
 */
inline void handle_m8_event_53_resp(const btt::proto::Frame& fr) {
  if (fr.payload.size() < 2) return;
  const uint8_t result = fr.payload[0];
  const uint8_t reason = fr.payload[1];
  if (result == 0x00) LOGI("0x53 ACK: seq=%u result=%u reason=0x%02X", fr.seq, result, reason);
  else                LOGW("0x53 ACK FAIL: seq=%u result=%u reason=0x%02X", fr.seq, result, reason);
}

/**
 * @brief 处理 `handle_m8_trigger_1C_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param outbound 输出队列。
 */
inline void handle_m8_trigger_1C_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                     btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  if (fr.payload.size() != 2) {
    result = 0x01; reason = 0x04; // 命令长度错误
  } else {
    const uint8_t sw = fr.payload[0];
    const uint8_t ps = fr.payload[1];
    if (sw > 2 || ps > 2) {
      result = 0x01; reason = 0x03; // 无效参数
    } else {
      // 本版本仅支持 1=上位机触发(TCP)，0/2 仅保存/回显（不生效），仍返回成功
      rt.m8_switch_trigger.store(sw);
      rt.m8_pass_trigger.store(ps);
      (void)m5_persist_save(rt);
    }
  }

  outbound.push(make_resp_2b(0x1C, fr.seq, result, reason));
}

/**
 * @brief 处理 `handle_m8_trigger_1D_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param outbound 输出队列。
 */
inline void handle_m8_trigger_1D_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                     btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;
  if (!fr.payload.empty()) { result = 0x01; reason = 0x04; }

  btt::proto::Frame resp;
  resp.cmd = 0x1D;
  resp.level = 0x01;
  resp.seq = fr.seq;
  resp.payload.reserve(4);
  resp.payload.push_back(result);
  resp.payload.push_back(reason);
  resp.payload.push_back(rt.m8_switch_trigger.load());
  resp.payload.push_back(rt.m8_pass_trigger.load());
  outbound.push(std::move(resp));
}

/**
 * @brief 处理 `handle_m8_strategy_28_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param event_busy 事件忙碌标志。
 * @param outbound 输出队列。
 */
inline void handle_m8_strategy_28_req(const btt::proto::Frame& fr, RuntimeConfig& rt, bool event_busy,
                                      btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  if (event_busy) {
    result = 0x01; reason = 0x30; // 资源被占用
    outbound.push(make_resp_2b(0x28, fr.seq, result, reason));
    return;
  }

  if (fr.payload.size() < 25) {
    result = 0x01; reason = 0x04; // 命令长度错误
    outbound.push(make_resp_2b(0x28, fr.seq, result, reason));
    return;
  }

  const uint8_t event_type = fr.payload[0];
  if (event_type > 1) {
    result = 0x01; reason = 0x40; // 无效的事件类型
    outbound.push(make_resp_2b(0x28, fr.seq, result, reason));
    return;
  }

  const uint8_t n = fr.payload[24];
  const size_t expect = 25 + size_t(n) * 12;
  if (fr.payload.size() != expect) {
    result = 0x01; reason = 0x04;
    outbound.push(make_resp_2b(0x28, fr.seq, result, reason));
    return;
  }

  // 校验生效子集：仅取第一个 video + 第一个 pic
  bool have_v = false;
  bool have_pic = false;
  const uint8_t cnt = (n > 4) ? 4 : n;
  for (uint8_t i = 0; i < cnt; ++i) {
    const size_t o = 25 + size_t(i) * 12;
    const uint8_t ft = fr.payload[o + 0];
    const uint16_t sd = u16_be(fr.payload.data() + o + 1);
    const uint16_t ed = u16_be(fr.payload.data() + o + 3);
    const uint16_t md = u16_be(fr.payload.data() + o + 5);

    if (ft == 1 && !have_v) {
      if (sd > kM8DelayMaxMs || ed > kM8DelayMaxMs) { result = 0x01; reason = 0x03; break; }
      if (!(md == 0 || md <= kM8VideoMaxDurMaxS)) { result = 0x01; reason = 0x03; break; }
      have_v = true;
    } else if (ft == 0 && !have_pic) {
      if (sd > kM8DelayMaxMs) { result = 0x01; reason = 0x03; break; }
      have_pic = true;
    }
  }

  if (result == 0x00) {
    // 保存 raw（固定 73B，查询时按 N 截断回显）
    uint8_t raw73[73]{};
    std::memset(raw73, 0, 73);
    const size_t cp = (fr.payload.size() > 73) ? 73 : fr.payload.size();
    std::memcpy(raw73, fr.payload.data(), cp);

    {
      std::lock_guard<std::mutex> lk(rt.m8_mu);
      if (event_type == 0) std::memcpy(rt.m8_strategy_raw0.data(), raw73, 73);
      else                 std::memcpy(rt.m8_strategy_raw1.data(), raw73, 73);
    }

    m8_apply_effective_from_strategy_raw(event_type, raw73, rt);
    (void)m5_persist_save(rt);
  }

  outbound.push(make_resp_2b(0x28, fr.seq, result, reason));
}

/**
 * @brief 处理 `handle_m8_strategy_29_req` 对应流程。
 * @param fr 协议帧。
 * @param rt 共享运行态。
 * @param outbound 输出队列。
 */
inline void handle_m8_strategy_29_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                      btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;
  btt::proto::Frame resp;
  resp.cmd = 0x29;
  resp.level = 0x01;
  resp.seq = fr.seq;

  uint8_t raw73[73]{};
  m8_fill_default_strategy_raw(0, raw73);

  uint8_t event_type = 0;
  if (fr.payload.size() == 1) {
    event_type = fr.payload[0];
    if (event_type <= 1) {
      std::lock_guard<std::mutex> lk(rt.m8_mu);
      if (event_type == 0) std::memcpy(raw73, rt.m8_strategy_raw0.data(), 73);
      else                 std::memcpy(raw73, rt.m8_strategy_raw1.data(), 73);
    } else {
      result = 0x01;
      reason = 0x40; // 无效的事件类型
      raw73[0] = event_type;
      raw73[24] = 0; // N=0，最短回显
    }
  } else {
    result = 0x01;
    reason = 0x04; // 命令长度错误
    raw73[0] = 0xFF;
    raw73[24] = 0;
  }

  const uint8_t n = raw73[24];
  const uint8_t cnt = (n > 4) ? 4 : n;
  const size_t len = 25 + size_t(cnt) * 12;
  resp.payload.reserve(2 + len);
  resp.payload.push_back(result);
  resp.payload.push_back(reason);
  resp.payload.insert(resp.payload.end(), raw73, raw73 + len);
  outbound.push(std::move(resp));
}

/**
 * @brief 执行 `m8_preempt_debug_rec_if_any` 相关内部逻辑。
 * @param rt 共享运行态。
 * @param media 媒体管理器。
 */
inline void m8_preempt_debug_rec_if_any(RuntimeConfig& rt, MediaManager& media) {
  uint32_t eid = 0;
  std::string dir;
  uint8_t lf = 0, lz = 0;
  if (!media.current_event(eid, dir, lf, lz)) return;

  std::vector<FileInfo25> vf;
  (void)media.stop_record(0, true, 0, 0, vf);

  // 尽量补齐 meta（保持 /data/events/<eid> 可查）
  if (eid != 0 && !dir.empty()) {
    EventMetaV1 em;
    std::vector<FileInfo25> old;
    if (!read_meta_v1(dir, em, old)) {
      em.event_id = eid;
      em.event_type = 2; // 上位机事件
      em.rsv0 = 0;
    }
    em.cur_pos = rt.position.load();
    build_bcdtime8_array(em.end_bcd);
    (void)write_meta_v1(dir, em, vf);
  }

  media.clear_current_event();
}

/**
 * @brief 处理 M8 操岔控制命令 `0x41`。
 * @note WHY: 结束流程被拆成“停录像 -> 上报 action=1 -> 延时补拍 -> 上报 action=2”，
 *       是因为协议侧需要区分“录像已经封口”和“文件列表已经稳定可查询”两个时刻；
 *       如果把它们压成一次动作，上位机会拿不到稳定的文件清单。
 */
inline void handle_m8_switch_control_41_req(
    const btt::proto::Frame& fr, RuntimeConfig& rt, MediaManager& media,
    RealTimeStreamer& streamer, EventUploader& uploader, M8ActiveEvent& st,
    std::atomic<uint32_t>& active_eid, std::atomic<uint8_t>& active_type,
    std::atomic<int64_t>& deadline_ms,
    btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  if (fr.payload.size() != 2) {
    result = 0x01; reason = 0x04;
    outbound.push(make_resp_2b(0x41, fr.seq, result, reason));
    return;
  }

  const uint8_t act = fr.payload[0]; // 0 start, 1 end
  if (!(act == 0 || act == 1)) {
    result = 0x01; reason = 0x03;
    outbound.push(make_resp_2b(0x41, fr.seq, result, reason));
    return;
  }

  if (act == 0) {
    // start
    if (st.active) {
      if (st.event_type == 0) {
        result = 0x01; reason = 0x50; // 正在扳动
        outbound.push(make_resp_2b(0x41, fr.seq, result, reason));
        return;
      }
      // P1 抢占 P2：结束当前过车（action=3），不发 action=1
      std::vector<FileInfo25> vids;
      (void)media.stop_record(0, true, 0, 0, vids);
      std::vector<FileInfo25> pics;
      (void)media.snap4_jpg_to_dir(rt, 0, 0, st.dir, pics);
      std::vector<FileInfo25> all = vids;
      all.insert(all.end(), pics.begin(), pics.end());
      outbound.push(make_m8_pass_event_52(rt, 3, st.event_id, st.start_bcd, all));

      EventMetaV1 em;
      em.event_id = st.event_id;
      em.event_type = 1;
      em.rsv0 = 0;
      em.cur_pos = rt.position.load();
      std::memcpy(em.start_bcd, st.start_bcd, 8);
      std::memcpy(em.end_bcd, st.start_bcd, 8);
      em.hydraulic = 0; em.level = 0; em.temp_humi = 0; em.gap_value = 0;
      (void)write_meta_v1(st.dir, em, all);

      media.clear_current_event();
      st = M8ActiveEvent{};
      active_eid.store(0);
      active_type.store(0xFF);
      deadline_ms.store(0);
    }

    // 抢占/终止 P3 长任务
    if (streamer.active()) streamer.stop_and_join(true, 0);
    if (uploader.busy()) (void)uploader.request_preempt(0x50);
    m8_preempt_debug_rec_if_any(rt, media);

    // 读取生效参数
    uint16_t timeout_s=60, sd=0, ed=0, md=15, picd=0;
    m8_get_effective_params(rt, 0, timeout_s, sd, ed, md, picd);

    media.set_video_max_ms(uint32_t(md) * 1000u);
    if (sd > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(sd));
    }

    uint32_t event_id = 0;
    std::string dir;
    const uint64_t need_bytes = est_event_B_bytes(rt.video_bitrate_kbps.load());
    if (!media.start_record(0, true, 0, 0, rt, need_bytes, event_id, dir)) {
      result = 0x01; reason = 0x01;
      outbound.push(make_resp_2b(0x41, fr.seq, result, reason));
      return;
    }

    uint8_t start_bcd[8]{};
    build_bcdtime8_array(start_bcd);

    // 写 meta（先空列表）
    EventMetaV1 em;
    em.event_id = event_id;
    em.event_type = 0; // 操岔
    em.rsv0 = 0;
    em.cur_pos = rt.position.load();
    std::memcpy(em.start_bcd, start_bcd, 8);
    std::memcpy(em.end_bcd, start_bcd, 8); // 本版本统一写开始时间
    em.hydraulic = 0; em.level = 0; em.temp_humi = 0; em.gap_value = 0;
    (void)write_meta_v1(dir, em, {});

    // 命令应答
    outbound.push(make_resp_2b(0x41, fr.seq, 0x00, 0x00));

    // 上报 action=0（开始）
    outbound.push(make_m8_switch_event_53(rt, 0, event_id, start_bcd, {}));

    // 记录活动事件
    st.active = true;
    st.event_type = 0;
    st.event_id = event_id;
    st.dir = dir;
    std::memcpy(st.start_bcd, start_bcd, 8);
    st.start_ms_steady = now_ms_steady();
    active_eid.store(event_id);
    active_type.store(0);
    deadline_ms.store(st.start_ms_steady + int64_t(timeout_s) * 1000);
    return;
  }

  // end
  if (!st.active || st.event_type != 0) {
    result = 0x01; reason = 0x02;
    outbound.push(make_resp_2b(0x41, fr.seq, result, reason));
    return;
  }

  // 命令应答
  outbound.push(make_resp_2b(0x41, fr.seq, 0x00, 0x00));

  // 正常结束：stop_delay -> stop_video -> action=1 -> pic_delay -> snap -> action=2
  uint16_t timeout_s=60, sd=0, ed=0, md=15, picd=0;
  m8_get_effective_params(rt, 0, timeout_s, sd, ed, md, picd);

  if (ed > 0) std::this_thread::sleep_for(std::chrono::milliseconds(ed));

  std::vector<FileInfo25> vids;
  (void)media.stop_record(0, true, 0, 0, vids);

  outbound.push(make_m8_switch_event_53(rt, 1, st.event_id, st.start_bcd, {}));

  if (picd > 0) std::this_thread::sleep_for(std::chrono::milliseconds(picd));

  std::vector<FileInfo25> pics;
  (void)media.snap4_jpg_to_dir(rt, 0, 0, st.dir, pics);

  std::vector<FileInfo25> all = vids;
  all.insert(all.end(), pics.begin(), pics.end());

  EventMetaV1 em;
  em.event_id = st.event_id;
  em.event_type = 0;
  em.rsv0 = 0;
  em.cur_pos = rt.position.load();
  std::memcpy(em.start_bcd, st.start_bcd, 8);
  std::memcpy(em.end_bcd, st.start_bcd, 8);
  em.hydraulic = 0; em.level = 0; em.temp_humi = 0; em.gap_value = 0;
  (void)write_meta_v1(st.dir, em, all);

  outbound.push(make_m8_switch_event_53(rt, 2, st.event_id, st.start_bcd, all));

  media.clear_current_event();
  st = M8ActiveEvent{};
  active_eid.store(0);
  active_type.store(0xFF);
  deadline_ms.store(0);
}

/**
 * @brief 处理 M8 过车触发命令 `0xA6`。
 * @note WHY: 过车和操岔共用同一套媒体资源，因此开始新过车前必须先抢占实时流、上传和调试录像；
 *       这不是“顺手清理资源”，而是为了把协议上的单活动事件约束映射到实际媒体设备上。
 */
inline void handle_m8_pass_trigger_A6_req(
    const btt::proto::Frame& fr, RuntimeConfig& rt, MediaManager& media,
    RealTimeStreamer& streamer, EventUploader& uploader, M8ActiveEvent& st,
    std::atomic<uint32_t>& active_eid, std::atomic<uint8_t>& active_type,
    std::atomic<int64_t>& deadline_ms,
    btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  if (fr.payload.size() != 1) {
    result = 0x01; reason = 0x04;
    outbound.push(make_resp_2b(0xA6, fr.seq, result, reason));
    return;
  }
  const uint8_t act = fr.payload[0]; // 0 start, 1 stop
  if (!(act == 0 || act == 1)) {
    result = 0x01; reason = 0x03;
    outbound.push(make_resp_2b(0xA6, fr.seq, result, reason));
    return;
  }

  if (act == 0) {
    // start
    if (st.active) {
      result = 0x01;
      reason = (st.event_type == 0) ? 0x50 : 0x51;
      outbound.push(make_resp_2b(0xA6, fr.seq, result, reason));
      return;
    }

    // 抢占/终止 P3 长任务
    if (streamer.active()) streamer.stop_and_join(true, 0);
    if (uploader.busy()) (void)uploader.request_preempt(0x51);
    m8_preempt_debug_rec_if_any(rt, media);

    // 读取生效参数
    uint16_t timeout_s=60, sd=0, ed=0, md=15, picd=0;
    m8_get_effective_params(rt, 1, timeout_s, sd, ed, md, picd);

    media.set_video_max_ms(uint32_t(md) * 1000u);
    if (sd > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(sd));
    }

    uint32_t event_id = 0;
    std::string dir;
    const uint64_t need_bytes = est_event_B_bytes(rt.video_bitrate_kbps.load());
    if (!media.start_record(0, true, 0, 0, rt, need_bytes, event_id, dir)) {
      result = 0x01; reason = 0x01;
      outbound.push(make_resp_2b(0xA6, fr.seq, result, reason));
      return;
    }

    uint8_t start_bcd[8]{};
    build_bcdtime8_array(start_bcd);

    // 写 meta（先空列表）
    EventMetaV1 em;
    em.event_id = event_id;
    em.event_type = 1; // 过车
    em.rsv0 = 0;
    em.cur_pos = rt.position.load();
    std::memcpy(em.start_bcd, start_bcd, 8);
    std::memcpy(em.end_bcd, start_bcd, 8);
    em.hydraulic = 0; em.level = 0; em.temp_humi = 0; em.gap_value = 0;
    (void)write_meta_v1(dir, em, {});

    // 命令应答
    outbound.push(make_resp_2b(0xA6, fr.seq, 0x00, 0x00));

    // 上报 action=0
    outbound.push(make_m8_pass_event_52(rt, 0, event_id, start_bcd, {}));

    st.active = true;
    st.event_type = 1;
    st.event_id = event_id;
    st.dir = dir;
    std::memcpy(st.start_bcd, start_bcd, 8);
    st.start_ms_steady = now_ms_steady();
    active_eid.store(event_id);
    active_type.store(1);
    deadline_ms.store(st.start_ms_steady + int64_t(timeout_s) * 1000);
    return;
  }

  // stop
  if (!st.active || st.event_type != 1) {
    result = 0x01; reason = 0x02;
    outbound.push(make_resp_2b(0xA6, fr.seq, result, reason));
    return;
  }

  // 命令应答
  outbound.push(make_resp_2b(0xA6, fr.seq, 0x00, 0x00));

  uint16_t timeout_s=60, sd=0, ed=0, md=15, picd=0;
  m8_get_effective_params(rt, 1, timeout_s, sd, ed, md, picd);

  if (ed > 0) std::this_thread::sleep_for(std::chrono::milliseconds(ed));

  std::vector<FileInfo25> vids;
  (void)media.stop_record(0, true, 0, 0, vids);

  outbound.push(make_m8_pass_event_52(rt, 1, st.event_id, st.start_bcd, {}));

  if (picd > 0) std::this_thread::sleep_for(std::chrono::milliseconds(picd));

  std::vector<FileInfo25> pics;
  (void)media.snap4_jpg_to_dir(rt, 0, 0, st.dir, pics);

  std::vector<FileInfo25> all = vids;
  all.insert(all.end(), pics.begin(), pics.end());

  EventMetaV1 em;
  em.event_id = st.event_id;
  em.event_type = 1;
  em.rsv0 = 0;
  em.cur_pos = rt.position.load();
  std::memcpy(em.start_bcd, st.start_bcd, 8);
  std::memcpy(em.end_bcd, st.start_bcd, 8);
  em.hydraulic = 0; em.level = 0; em.temp_humi = 0; em.gap_value = 0;
  (void)write_meta_v1(st.dir, em, all);

  outbound.push(make_m8_pass_event_52(rt, 2, st.event_id, st.start_bcd, all));

  media.clear_current_event();
  st = M8ActiveEvent{};
  active_eid.store(0);
  active_type.store(0xFF);
  deadline_ms.store(0);
}

/**
 * @brief 处理 M8 内部超时事件。
 * @note WHY: 超时统一走 `action=3`，且显式跳过正常结束时的 `action=1`，
 *       因为它表达的是“策略超时导致的异常收束”，必须和“人工正常结束”在协议语义上可区分。
 */
inline void handle_m8_internal_timeout(
    const btt::proto::Frame& fr, RuntimeConfig& rt, MediaManager& media,
    M8ActiveEvent& st, std::atomic<uint32_t>& active_eid,
    std::atomic<uint8_t>& active_type, std::atomic<int64_t>& deadline_ms,
    btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  if (fr.payload.size() < 5) return;
  const uint32_t eid = read_u32_be(fr.payload.data());
  const uint8_t et = fr.payload[4];

  if (!st.active || st.event_id != eid || st.event_type != et) return;

  // 超时：action=3，带文件信息；不发 action=1
  std::vector<FileInfo25> vids;
  (void)media.stop_record(0, true, 0, 0, vids);
  std::vector<FileInfo25> pics;
  (void)media.snap4_jpg_to_dir(rt, 0, 0, st.dir, pics);
  std::vector<FileInfo25> all = vids;
  all.insert(all.end(), pics.begin(), pics.end());

  if (et == 0) outbound.push(make_m8_switch_event_53(rt, 3, st.event_id, st.start_bcd, all));
  else         outbound.push(make_m8_pass_event_52(rt, 3, st.event_id, st.start_bcd, all));

  EventMetaV1 em;
  em.event_id = st.event_id;
  em.event_type = et;
  em.rsv0 = 0;
  em.cur_pos = rt.position.load();
  std::memcpy(em.start_bcd, st.start_bcd, 8);
  std::memcpy(em.end_bcd, st.start_bcd, 8);
  em.hydraulic = 0; em.level = 0; em.temp_humi = 0; em.gap_value = 0;
  (void)write_meta_v1(st.dir, em, all);

  media.clear_current_event();
  st = M8ActiveEvent{};
  active_eid.store(0);
  active_type.store(0xFF);
  deadline_ms.store(0);
}

// ----------------- 主运行：认证/心跳 + 控制(0x16) + M2.1(0x20/0x21) + M3(0x30/0x39/0x3A) + M4(0x3D/0x3E/0x3F) + M5(0x11/0x12/0x1A/0x1B/0x38 + 0x5B) + M6(事件文件上传) + M7(0x35/0x36/0x37/0x51) + M8(0x1C/0x1D/0x28/0x29/0x41/0xA6/0x52/0x53) -----------------
/**
 * @brief 运行 btt_core 主循环。
 * @param def 启动默认配置。
 * @param rt 共享运行态。
 * @param stop_flag 停止标志。
 * @return 返回计算结果。
 */
int RunCore(const DefaultConfig& def, RuntimeConfig& rt,
            std::atomic<bool>& stop_flag) {
  btt::net::TcpClient cli;
  btt::proto::StreamDecoder decoder;

  btt::utils::BlockingQueue<btt::proto::Frame> inbound;
  btt::utils::BlockingQueue<btt::proto::Frame> outbound;

  // M3：媒体管理器（拍照/录像）
  MediaManager media;

  // M7：实时流管理器
  RealTimeStreamer streamer;

  // M6：事件文件上传管理器
  EventUploader uploader;

  // M8：操岔/过车活动事件状态（actor 线程维护；timer 线程仅看原子触发超时）
  M8ActiveEvent m8_state;
  std::atomic<uint32_t> m8_active_eid{0};
  std::atomic<uint8_t>  m8_active_type{0xFF};
  std::atomic<int64_t>  m8_deadline_ms{0};

  // M5：加载持久化配置 + misc 初始化
  hgs_misc_init();
  m5_load_into_runtime(def, rt);
  {
    uint8_t boot_reason = 0;
    if (load_boot_reconnect_reason(boot_reason)) {
      rt.reconnect_reason.store(boot_reason);
      clear_boot_reconnect_reason();
      LOGI("BOOT reconnect reason restored: %u", unsigned(boot_reason));
    }
  }
  (void)m5_apply_persisted_eth0_if_ready(rt);

  // M7：绑定实时流到媒体回调与控制通道
  streamer.attach(rt, outbound);
  media.set_streamer(&streamer);

  std::atomic<bool> stop{false};
  std::atomic<uint8_t> pending_control_action{
      static_cast<uint8_t>(CoreControlAction::None)};
  std::atomic<uint16_t> pending_control_seq{0};
  std::mutex pending_net_mu;
  PendingNetConfig pending_net_cfg;
  std::atomic<bool> pending_net_active{false};
  std::atomic<uint16_t> pending_net_seq{0};
  std::atomic<uint8_t> final_control_action{
      static_cast<uint8_t>(CoreControlAction::None)};
  UpgradedWatchdogClient watchdog_client;
  UpgradedControlClient upgrade_ctl_client;
  UpgradeIngress upgrade_ingress;
  bool upgrade_report_sent_for_auth = false;
  int64_t last_watchdog_tx_ms = 0;
  constexpr uint32_t kTimerWatchdogStepMs = 50;
  constexpr int kReconnectConnectTimeoutMs = 500;

  // init runtime
  rt.switch_model.store(def.switch_model);
  rt.last_rx_ms.store(now_ms_steady());
  rt.last_hb_tx_ms.store(now_ms_steady());
  rt.wd_net_rx_ms.store(now_ms_steady());
  rt.wd_actor_ms.store(now_ms_steady());
  rt.wd_net_tx_ms.store(now_ms_steady());
  rt.wd_timer_ms.store(now_ms_steady());
  
  auto service_timer_watchdog = [&] {
    const int64_t now = now_ms_steady();
    rt.wd_timer_ms.store(now);

    if (now - last_watchdog_tx_ms < upgraded::kWatchdogHeartbeatIntervalMs) {
      return;
    }

    upgraded::WatchdogPingV1 ping{};
    upgraded::InitWatchdogPing(&ping);
    ping.seq = rt.wd_ping_seq.fetch_add(1);
    ping.pid = static_cast<uint32_t>(::getpid());
    ping.monotonic_ms = static_cast<uint64_t>(now);
    ping.flags = 0;
    if (rt.connected.load()) ping.flags |= upgraded::kWatchdogFlagConnected;
    if (rt.authed.load()) ping.flags |= upgraded::kWatchdogFlagAuthed;
    ping.thread_bitmap = upgraded::kWatchdogThreadMaskRequired;
    ping.net_rx_age_ms = age_ms_from(now, rt.wd_net_rx_ms.load());
    ping.actor_age_ms = age_ms_from(now, rt.wd_actor_ms.load());
    ping.net_tx_age_ms = age_ms_from(now, rt.wd_net_tx_ms.load());
    ping.timer_age_ms = age_ms_from(now, rt.wd_timer_ms.load());
    (void)watchdog_client.SendPing(ping);
    last_watchdog_tx_ms = now;
  };

  auto timer_wait_watchdog_friendly = [&](uint32_t wait_ms) {
    uint32_t remaining_ms = wait_ms;
    while (!stop.load()) {
      if (stop_flag.load()) {
        stop.store(true);
        inbound.notify_all();
        outbound.notify_all();
        return false;
      }

      service_timer_watchdog();
      if (remaining_ms == 0) return true;

      const uint32_t step_ms = std::min<uint32_t>(remaining_ms, kTimerWatchdogStepMs);
      std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
      remaining_ms -= step_ms;
    }
    return false;
  };


  auto disconnect = [&](uint8_t reason){
    LOGW("DISCONNECT reason=%u", reason);
    cli.close_fd();
    upgrade_ingress.AbortActiveTransfer();
    pending_control_action.store(static_cast<uint8_t>(CoreControlAction::None));
    pending_control_seq.store(0);
    pending_net_active.store(false);
    pending_net_seq.store(0);

  // M6：停止上传线程
  uploader.stop_and_join();
  // M7：停止实时流（不通知）
  streamer.stop_and_join(false, 0);
    rt.connected.store(false);
    rt.authed.store(false);
    rt.hb_miss.store(0);
    decoder.reset();
    upgrade_report_sent_for_auth = false;
    rt.reconnect_reason.store(reason);
  };

  // NetRx
  std::thread t_rx([&]{
    uint8_t buf[2048];
    while (!stop.load()) {
      rt.wd_net_rx_ms.store(now_ms_steady());

      if (!rt.connected.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        continue;
      }

      ssize_t n = cli.recv_some(buf, sizeof(buf));
      if (n > 0) {
        rt.last_rx_ms.store(now_ms_steady());
        rt.hb_miss.store(0);
        rt.wd_net_rx_ms.store(now_ms_steady());

        auto frames = decoder.push(buf, (size_t)n);
        for (auto& fr : frames) inbound.push(std::move(fr));
        continue;
      }

      if (n == 0) { disconnect(14); continue; }
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        rt.wd_net_rx_ms.store(now_ms_steady());
        continue;
      }
      const int saved_errno = cli.last_error() ? cli.last_error() : errno;
      disconnect(reconnect_reason_from_socket_error(saved_errno));
    }
  });

  // Actor
  std::thread t_actor([&]{
    while (!stop.load()) {
      rt.wd_actor_ms.store(now_ms_steady());

      auto opt =
          inbound.pop_wait_for(stop, std::chrono::milliseconds(200));
      if (!opt.has_value()) {
        if (stop.load()) break;
        rt.wd_actor_ms.store(now_ms_steady());
        continue;
      }
      const auto& fr = *opt;



      // 开发期打印：优先看是否协议对齐（cmd/level/seq/len + raw）
      log_frame_detail("RX", fr);
      rt.wd_actor_ms.store(now_ms_steady());

      // 设备侧：收到0x00/0x01 一律按服务器应答处理
      if (fr.cmd == 0x00) {
        const bool was_authed = rt.authed.load();
        handle_auth_resp(fr, rt);
        if (!rt.authed.load()) disconnect(19);
        if (!was_authed && rt.authed.load() && !upgrade_report_sent_for_auth) {
          upgrade_report_sent_for_auth = maybe_send_upgrade_result_after_auth(
              rt, upgrade_ctl_client, outbound);
        }
        continue;
      }
      if (fr.cmd == 0x01) {
        handle_heartbeat_resp(fr, rt);
        continue;
      }

      // 内部：M8 超时触发
      if (fr.cmd == kCmdInternalM8Timeout) {
        handle_m8_internal_timeout(fr, rt, media, m8_state, m8_active_eid, m8_active_type, m8_deadline_ms, outbound);
        continue;
      }

      if (fr.cmd == 0x3C) {
        outbound.push(upgrade_ingress.HandlePacket(fr, upgrade_ctl_client));
        continue;
      }
      if (fr.cmd == 0x58) {
        handle_upgrade_result_58_resp(fr, upgrade_ctl_client);
        continue;
      }

      if (upgrade_ingress.quiet_mode() &&
          should_reject_cmd_during_upgrade_quiet(fr.cmd)) {
        LOGW("upgrade quiet mode reject cmd=0x%02X seq=%u", fr.cmd, fr.seq);
        outbound.push(make_resp_2b(fr.cmd, fr.seq, 0x01, 0x02));
        continue;
      }

      // 控制：0x16 重启 / 恢复出厂设置
      if (fr.cmd == 0x16) {
        handle_control_16_req(fr, def, rt, media, pending_control_action,
                              pending_control_seq, outbound);
        continue;
      }

      // M8：0x1C/0x1D/0x28/0x29 + 0x41/0xA6 + 0x52/0x53 ACK
      if (fr.cmd == 0x1C) { handle_m8_trigger_1C_req(fr, rt, outbound); continue; }
      if (fr.cmd == 0x1D) { handle_m8_trigger_1D_req(fr, rt, outbound); continue; }
      if (fr.cmd == 0x28) { handle_m8_strategy_28_req(fr, rt, m8_state.active, outbound); continue; }
      if (fr.cmd == 0x29) { handle_m8_strategy_29_req(fr, rt, outbound); continue; }
      if (fr.cmd == 0x41) { handle_m8_switch_control_41_req(fr, rt, media, streamer, uploader, m8_state, m8_active_eid, m8_active_type, m8_deadline_ms, outbound); continue; }
      if (fr.cmd == 0xA6) { handle_m8_pass_trigger_A6_req(fr, rt, media, streamer, uploader, m8_state, m8_active_eid, m8_active_type, m8_deadline_ms, outbound); continue; }
      if (fr.cmd == 0x52) { handle_m8_event_52_resp(fr); continue; }
      if (fr.cmd == 0x53) { handle_m8_event_53_resp(fr); continue; }

      // M5：0x11/0x12/0x1A/0x1B/0x38 + 0x5B ACK
      if (fr.cmd == 0x11) {
        handle_set_netparam_11_req(fr, rt, pending_net_mu, pending_net_cfg,
                                   pending_net_active, pending_net_seq,
                                   outbound);
        continue;
      }
      if (fr.cmd == 0x12) { handle_get_netparam_12_req(fr, rt, outbound); continue; }
      if (fr.cmd == 0x1A) { handle_set_media_1A_req(fr, rt, media, outbound); continue; }
      if (fr.cmd == 0x1B) { handle_get_media_1B_req(fr, rt, outbound); continue; }
      if (fr.cmd == 0x38) { handle_measure_38_req(fr, rt, media, outbound); continue; }
      if (fr.cmd == 0x5B) { handle_measure_event_5B_resp(fr, rt); continue; }

      // M2.1：0x20/0x21
      if (fr.cmd == 0x20) { handle_set_devcfg_req(fr, rt, outbound); continue; }
      if (fr.cmd == 0x21) { handle_get_devcfg_req(fr, rt, outbound); continue; }

      // M3：0x30/0x39/0x3A
      if (fr.cmd == 0x30) { handle_snap30_req(fr, rt, media, outbound); continue; }
      if (fr.cmd == 0x39) { handle_rec39_start_req(fr, rt, media, outbound); continue; }
      if (fr.cmd == 0x3A) { handle_rec3A_stop_req(fr, rt, media, outbound); continue; }

      // M7：0x35/0x36/0x37
      if (fr.cmd == 0x35) { handle_stream_open_35_req(fr, rt, media, streamer, outbound); continue; }
      if (fr.cmd == 0x36) { handle_stream_close_36_req(fr, streamer, outbound); continue; }
      if (fr.cmd == 0x37) { handle_stream_end_37_resp(fr); continue; }


      // M6：0x31/0x33（事件文件上传）
      if (fr.cmd == 0x31) { handle_event_upload_31_req(fr, rt, uploader, outbound, stop_flag); continue; }
      if (fr.cmd == 0x33) { handle_event_upload_33_req(fr, uploader, outbound); continue; }
      if (fr.cmd == 0x32) {
        // 0x32 为设备主动上报结束；上位机 ACK 走同一控制通道（payload=2B），此处仅做记录
        if (fr.payload.size() >= 2) {
          LOGI("0x32 EVENT_UPLOAD_END ACK: result=%u reason=0x%02X", fr.payload[0], fr.payload[1]);
        } else {
          LOGI("0x32 EVENT_UPLOAD_END ACK: payload_len=%zu", fr.payload.size());
        }
        continue;
      }

      // M4：0x3D/0x3E/0x3F
      if (fr.cmd == 0x3D) { handle_event_list_3D_req(fr, outbound); continue; }
      if (fr.cmd == 0x3E) { handle_event_detail_3E_req(fr, outbound); continue; }
      if (fr.cmd == 0x3F) { handle_event_clear_3F_req(fr, outbound); continue; }

      LOGW("RX unknown cmd=0x%02X level=0x%02X seq=%u len=%zu", fr.cmd, fr.level, fr.seq, fr.payload.size());
    }
  });

  // NetTx
  std::thread t_tx([&]{
    while (!stop.load()) {
      rt.wd_net_tx_ms.store(now_ms_steady());

      auto opt =
          outbound.pop_wait_for(stop, std::chrono::milliseconds(200));
      if (!opt.has_value()) {
        if (stop.load()) break;
        rt.wd_net_tx_ms.store(now_ms_steady());
        continue;
      }
      const bool is_pending_control_ack =
          opt->cmd == 0x16 &&
          opt->level == 0x01 &&
          pending_control_action.load() !=
              static_cast<uint8_t>(CoreControlAction::None) &&
          opt->seq == pending_control_seq.load();
      const bool is_pending_net_ack =
          opt->cmd == 0x11 &&
          opt->level == 0x01 &&
          pending_net_active.load() &&
          opt->seq == pending_net_seq.load();
      if (!rt.connected.load()) {
        if (is_pending_control_ack) {
          pending_control_action.store(
              static_cast<uint8_t>(CoreControlAction::None));
          pending_control_seq.store(0);
        }
        if (is_pending_net_ack) {
          pending_net_active.store(false);
          pending_net_seq.store(0);
        }
        continue;
      }

      // 编码前打印
      log_frame_brief("TX", *opt);

      auto bytes = btt::proto::encode(*opt);
      if (bytes.empty()) continue;

      // 编码后 dump 整帧，方便与上位机/抓包对照
      log_hex("TX RAW", bytes.data(), bytes.size());

      if (!cli.send_all(bytes.data(), bytes.size())) {
        const int saved_errno = cli.last_error() ? cli.last_error() : errno;
        disconnect(reconnect_reason_from_socket_error(saved_errno));
      } else {
        rt.wd_net_tx_ms.store(now_ms_steady());
        if (is_pending_net_ack) {
          PendingNetConfig staged{};
          {
            std::lock_guard<std::mutex> lk(pending_net_mu);
            staged = pending_net_cfg;
          }
          pending_net_active.store(false);
          pending_net_seq.store(0);
          commit_pending_net_config_after_ack(staged, rt);
        }
        if (is_pending_control_ack) {
          const uint8_t action =
              pending_control_action.exchange(
                  static_cast<uint8_t>(CoreControlAction::None));
          pending_control_seq.store(0);
          if (action != static_cast<uint8_t>(CoreControlAction::None)) {
            final_control_action.store(action);
            disconnect(5);
            stop.store(true);
            inbound.notify_all();
            outbound.notify_all();
          }
        }
      }
    }
  });

  // Timer：连接/认证/心跳
  std::thread t_timer([&]{
    while (!stop.load()) {
      if (!timer_wait_watchdog_friendly(0)) break;

      // M8：事件超时触发（只触发一次，交由 actor 串行收尾）
      {
        const uint32_t eid = m8_active_eid.load();
        const int64_t dl = m8_deadline_ms.load();
        if (eid != 0 && dl > 0) {
          const int64_t now = now_ms_steady();
          if (now >= dl) {
            int64_t expected = dl;
            if (m8_deadline_ms.compare_exchange_strong(expected, 0)) {
              btt::proto::Frame tf;
              tf.cmd = kCmdInternalM8Timeout;
              tf.level = 0xFF;
              tf.seq = 0;
              tf.payload.reserve(5);
              append_u32_be(tf.payload, eid);
              tf.payload.push_back(m8_active_type.load());
              inbound.push(std::move(tf));
            }
          }
        }
      }

      if (rt.force_disconnect.exchange(false)) {
        disconnect(rt.force_disconnect_reason.load());
        if (!timer_wait_watchdog_friendly(200)) break;
        continue;
      }

      if (rt.reconnect_disabled.load()) {
        if (!timer_wait_watchdog_friendly(200)) break;
        continue;
      }

      if (!rt.connected.load()) {
        const uint8_t ri = rt.reconnect_interval_s.load();
        if (ri == 0) {
          if (!timer_wait_watchdog_friendly(200)) break;
          continue;
        }

        {
        const uint32_t hip = rt.m5_host_ip_be.load();
        const uint16_t hpt = rt.m5_host_port.load();
        const std::string host_ip = ip_be_to_string(hip);
        LOGI("CONNECT to %s:%u ...", host_ip.c_str(), hpt);
        service_timer_watchdog();
        if (cli.connect_to(host_ip, hpt, kReconnectConnectTimeoutMs)) {
          LOGI("CONNECTED");
          rt.connected.store(true);
          rt.authed.store(false);
          rt.hb_miss.store(0);
          rt.last_rx_ms.store(now_ms_steady());
          decoder.reset();

          auto auth = make_auth_req(def, rt);
          rt.last_auth_tx_ms.store(now_ms_steady());
          outbound.push(std::move(auth));
        } else {
          const int saved_errno = cli.last_error();
          if (is_network_drop_errno(saved_errno)) {
            rt.reconnect_reason.store(13);
          }
          LOGW("CONNECT FAIL errno=%d sleep %us", saved_errno, ri);
          if (!timer_wait_watchdog_friendly(static_cast<uint32_t>(ri) * 1000u)) {
            break;
          }
        }
        }
        continue;
      }

      // 未认证：认证超时断链
      if (!rt.authed.load()) {
        const int64_t now = now_ms_steady();
        if (now - rt.last_auth_tx_ms.load() > def.auth_timeout_ms) {
          disconnect(20);
        }
        if (!timer_wait_watchdog_friendly(50)) break;
        continue;
      }

      // 心跳：空闲 >= hb 开始发，连发3次，hb*4 算超时断链
      const uint8_t hb = rt.hb_interval_s.load();
      if (hb > 0) {
        const int64_t now = now_ms_steady();
        const int64_t idle = now - rt.last_rx_ms.load();

        if (idle >= int64_t(hb) * 1000) {
          int miss = rt.hb_miss.load();

          if (miss < 3) {
            const int64_t need_idle = int64_t(hb) * 1000 * (miss + 1);
            const int64_t since_tx = now - rt.last_hb_tx_ms.load();
            if (idle >= need_idle && since_tx >= int64_t(hb) * 1000) {
              outbound.push(make_heartbeat_req(rt));
              rt.last_hb_tx_ms.store(now);
              rt.hb_miss.store(miss + 1);
            }
          } else {
            if (idle >= int64_t(hb) * 1000 * 4) {
              disconnect(17);
            }
          }
        }
      }

      // M7：实时流 keepalive 检查
      streamer.tick_keepalive();

      if (!timer_wait_watchdog_friendly(50)) break;
    }
  });



  // M5：周期测量线程（0x38 mode=0 配置后生效；整点对齐）
  std::thread t_measure([&]{
    std::time_t last_periodic_slot_t = 0;
    while (!stop.load()) {
      if (stop_flag.load()) break;

      if (!rt.m5_measure_periodic.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        continue;
      }

      const uint8_t interval = rt.m5_measure_interval_min.load();
      if (interval == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        continue;
      }

      // 计算下一个“整点基线”触发点
      std::time_t now_t = ::time(nullptr);
      std::tm tm{};
      ::localtime_r(&now_t, &tm);

      const int cur_min = tm.tm_min;
      const int cur_sec = tm.tm_sec;
      const bool at_boundary = ((cur_min % interval) == 0 && cur_sec == 0);

      std::tm slot_tm = tm;
      slot_tm.tm_min = (cur_min / interval) * interval;
      slot_tm.tm_sec = 0;
      const std::time_t cur_slot_t = ::mktime(&slot_tm);

      std::time_t next_t = 0;
      if (at_boundary && cur_slot_t > last_periodic_slot_t) {
        next_t = cur_slot_t; // 当前槽位尚未执行，立即触发
      } else {
        int next_min = ((cur_min / interval) + 1) * interval;
        if (next_min >= 60) { next_min = 0; tm.tm_hour += 1; }
        tm.tm_min = next_min;
        tm.tm_sec = 0;
        next_t = ::mktime(&tm);
        if (next_t <= now_t) next_t = now_t + 1;
      }

      // 睡眠直到 next_t（分段睡，便于 stop/配置变更）
      while (!stop.load()) {
        if (stop_flag.load()) break;
        if (!rt.m5_measure_periodic.load()) break;
        std::time_t tnow = ::time(nullptr);
        if (tnow >= next_t) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      }
      if (stop.load() || stop_flag.load() || !rt.m5_measure_periodic.load()) continue;

      if (next_t <= last_periodic_slot_t) continue;
      last_periodic_slot_t = next_t;

      // 触发一次周期测量
      const bool connected = rt.connected.load() && rt.authed.load();

      uint32_t event_id = 0;
      std::string dir;
      std::vector<FileInfo25> files;

      // 录像中视为“主要任务”，上报中断不带文件
      bool recording = false;
      {
        // 通过 current_event 判断：只要当前有录制事件
        uint32_t eid=0; std::string d; uint8_t lf=0,lz=0;
        recording = media.current_event(eid, d, lf, lz);
      }

      uint8_t action = 2; // 数据采集结束
      if (recording) action = 3; // 中断

      if (action == 3) {
        // 仍创建一个空事件目录，供 M4 查询/排查
        const uint64_t need_bytes = kMetaMaxBytes;
        uint32_t eid = 0; std::string edir;
        if (media.create_event_dir(need_bytes, eid, edir)) {
          EventMetaV1 em; em.event_id=eid; em.event_type=3; em.rsv0=0; em.cur_pos=rt.position.load();
          build_bcdtime8_array(em.start_bcd); build_bcdtime8_array(em.end_bcd);
          em.hydraulic=0xFFFF; em.level=0xFFFF; em.temp_humi=0xFFFFFFFFu; em.gap_value=0xFFFFFFFFu;
          (void)write_meta_v1(edir, em, {});
          if (connected) {
            auto f = make_measure_event_5B_req(rt, 0, action, eid, {});
            outbound.push(std::move(f));
          }
        }
        continue;
      }

      const uint64_t need_bytes = est_event_A_bytes();
      if (!media.measure_snap4_to_event(rt, 0, 0, need_bytes, event_id, dir, files)) {
        LOGW("periodic measure: capture failed");
        continue;
      }

      // 写 meta.bin
      EventMetaV1 em;
      em.event_id = event_id;
      em.event_type = 3;
      em.rsv0 = 0;
      em.cur_pos = rt.position.load();
      if (!files.empty()) {
        std::memcpy(em.start_bcd, files[0].start_bcd, 8);
        std::memcpy(em.end_bcd, files[0].start_bcd, 8);
      }
      em.hydraulic = 0xFFFF;
      em.level = 0xFFFF;
      em.temp_humi = 0xFFFFFFFFu;
      em.gap_value = 0xFFFFFFFFu;
      (void)write_meta_v1(dir, em, files);

      if (connected) {
        auto f = make_measure_event_5B_req(rt, 0, action, event_id, files);
        outbound.push(std::move(f));
      }
    }
  });
  // wait exit
  while (!stop.load()) {

    if (stop_flag.load()) {
      stop.store(true);
      inbound.notify_all();
      outbound.notify_all();
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  inbound.notify_all();
  outbound.notify_all();
  cli.close_fd();
  streamer.stop_and_join(false, 0);

  if (t_timer.joinable()) t_timer.join();
  if (t_rx.joinable()) t_rx.join();
  if (t_actor.joinable()) t_actor.join();
  if (t_tx.joinable()) t_tx.join();
  if (t_measure.joinable()) t_measure.join();

  const uint8_t action = final_control_action.load();
  if (action == static_cast<uint8_t>(CoreControlAction::RestartProgram)) {
    if (!save_boot_reconnect_reason(5)) {
      LOGW("persist boot reconnect reason failed: reason=5 errno=%d", errno);
    }
    return kRunCoreExitRestartProgram;
  }
  if (action == static_cast<uint8_t>(CoreControlAction::RebootDevice)) {
    if (!save_boot_reconnect_reason(5)) {
      LOGW("persist boot reconnect reason failed: reason=5 errno=%d", errno);
    }
    return kRunCoreExitRebootDevice;
  }
  return 0;
}

}  // namespace btt::core
