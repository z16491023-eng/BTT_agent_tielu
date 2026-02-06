#pragma once
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
#include <sys/ioctl.h>
#include <net/if.h>
#include <linux/route.h>
#include <errno.h>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <sys/wait.h>





// 多媒体库（参考 agent.c / HiF_media_ss522.h）
extern "C" {
#include "HiF_media_ss522.h"
#include "hgs_misc.h"
}

#include "libs/utils/log.hpp"
#include "libs/utils/blocking_queue.hpp"
#include "libs/proto/btt_proto.hpp"
#include "libs/net/tcp_client.hpp"

namespace btt::core {

using namespace std::chrono;

// ---------- 基础 ----------
inline int64_t now_ms_steady() {
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}


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
  uint8_t  pos_mask = 0;   // Bit0..Bit3：定位/反位/锁舌/油杯
  uint16_t reserved = 0;//保留
  uint8_t  file_type = 0;  // Bit7激光 Bit6补光 Bit4-5 camId 低4位文件类型(0图片/1录像/5配置)
};

inline bool write_meta_v1(const std::string& dir, const EventMetaV1& m, const std::vector<FileInfo25>& files);
inline bool read_meta_v1(const std::string& dir, EventMetaV1& m, std::vector<FileInfo25>& files);
// ---------- 开发期调试：hex dump ----------
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

inline void log_frame_brief(const char* dir, const btt::proto::Frame& fr) {
  LOGI("%s frame: cmd=0x%02X level=0x%02X seq=%u payload_len=%zu", dir, fr.cmd, fr.level, fr.seq, fr.payload.size());
}

inline void log_frame_detail(const char* dir, const btt::proto::Frame& fr) {
  log_frame_brief(dir, fr);
  if (!fr.raw.empty()) {
    log_hex((std::string(dir) + " RAW").c_str(), fr.raw.data(), fr.raw.size());
  } else {
    // RX 没有 raw 时，至少 dump payload
    if (!fr.payload.empty()) log_hex((std::string(dir) + " PAYLOAD").c_str(), fr.payload.data(), fr.payload.size());
  }
}

// ---------- 配置结构 ----------
struct DefaultConfig {
  std::string server_ip = "192.168.1.200";
  uint16_t server_port = 9000;

  // 设备序列号 5B
  uint8_t sn_model = 0x00;
  uint8_t sn_year  = 0x00;
  uint8_t sn_month = 0x00;
  uint16_t sn_batch = 0x0001;

  // 设备版本 3B
  uint8_t ver_major = 0x00;
  uint8_t ver_minor = 0x00;
  uint8_t ver_tag   = 0x01;

  uint8_t switch_model = 0x01;  // 转辙机型号
  uint8_t run_mode = 0x00;      // 0正常 1测试

  // 认证超时（ms）
  int auth_timeout_ms = 5000;
};

struct RuntimeConfig {
  std::atomic<bool> connected{false};
  std::atomic<bool> authed{false};
  std::atomic<bool> reconnect_disabled{false};

  std::atomic<uint16_t> seq{1};

  std::atomic<uint8_t> hb_interval_s{5};
  std::atomic<uint8_t> reconnect_interval_s{5};
  std::atomic<uint8_t> no_comm_reboot_s{0};
  std::atomic<uint8_t> business_timeout_s{0};

  std::atomic<uint8_t> switch_model{0x01};

  std::atomic<uint8_t> position{3};          // 0/1/2/3
  std::atomic<uint8_t> reconnect_reason{0};  // 0..20

  std::atomic<int> hb_miss{0};
  std::atomic<int64_t> last_rx_ms{0};
  std::atomic<int64_t> last_hb_tx_ms{0};
  std::atomic<int64_t> last_auth_tx_ms{0};

  // 媒体参数（用于容量模型/准入控制）
  std::atomic<uint16_t> video_bitrate_kbps{560}; // 默认 560kbps，上位机可配置（>1024 将拒绝）


  // 0x20 原始配置 36B
  std::atomic<bool> devcfg_valid{false};
  std::mutex devcfg_mu;
  std::array<uint8_t, 36> devcfg_raw{};

  // ---------------- M5：网络/媒体/测量配置 ----------------
  // 0x11/0x12 网络参数（IP 均为 BE 整数，例如 192.168.1.1 => 0xC0A80101）
  std::atomic<uint32_t> m5_dev_ip_be{0};
  std::atomic<uint32_t> m5_gateway_be{0};
  std::atomic<uint32_t> m5_netmask_be{0};

  std::atomic<uint32_t> m5_host_ip_be{0};
  std::atomic<uint16_t> m5_host_port{0};

  std::atomic<uint32_t> m5_upgrade_server_ip_be{0};
  std::atomic<uint32_t> m5_debug_server_ip_be{0};
  std::atomic<uint16_t> m5_debug_server_port{0};
  std::atomic<uint8_t>  m5_debug_proto{0}; // 0:TCP 1:UDP

  // 0x1A/0x1B 媒体参数
  std::atomic<uint8_t> m5_pic_res{3};
  std::atomic<uint8_t> m5_pic_quality{75};
  std::atomic<uint8_t> m5_video_res{3};
  std::atomic<uint8_t> m5_video_fps{25};
  std::atomic<uint8_t> m5_video_bitrate_unit{18}; // 18*32=576kbps

  // 0x38 周期测量配置
  std::atomic<bool>     m5_measure_periodic{false};
  std::atomic<uint8_t>  m5_measure_interval_min{30};

  // 强制断链重连（用于切换 host ip/port 或应用网络参数）
  std::atomic<bool>     force_disconnect{false};
  std::atomic<uint8_t>  force_disconnect_reason{0};

  // ---------------- M8：触发方式/事件策略 ----------------
  // 0x1C/0x1D：触发方式（本版本仅支持 1=上位机触发(TCP)，其它值仅保存/回显，不生效）
  std::atomic<uint8_t> m8_switch_trigger{1};
  std::atomic<uint8_t> m8_pass_trigger{1};

  // 0x28/0x29：策略原始配置（最大 25+12*4=73B），用于回显
  mutable std::mutex m8_mu;
  std::array<uint8_t, 73> m8_strategy_raw0{}; // event_type=0(操岔)
  std::array<uint8_t, 73> m8_strategy_raw1{}; // event_type=1(过车)

  // 生效参数（从 raw 解析得到；仅取第一个 video + 第一个 pic）
  std::atomic<uint16_t> m8_timeout_s0{60};
  std::atomic<uint16_t> m8_timeout_s1{60};

  std::atomic<uint16_t> m8_v_start_delay_ms0{0};
  std::atomic<uint16_t> m8_v_stop_delay_ms0{0};
  std::atomic<uint16_t> m8_v_max_dur_s0{15};
  std::atomic<uint16_t> m8_pic_delay_ms0{0};

  std::atomic<uint16_t> m8_v_start_delay_ms1{0};
  std::atomic<uint16_t> m8_v_stop_delay_ms1{0};
  std::atomic<uint16_t> m8_v_max_dur_s1{15};
  std::atomic<uint16_t> m8_pic_delay_ms1{0};
};




// 前置声明（M5 持久化依赖）
inline bool ensure_dir(const std::string& path);

// ---------------- M5：持久化配置（/data/m5_cfg.bin） ----------------
static constexpr const char* kM5CfgPath = "/data/m5_cfg.bin";

inline std::string ip_be_to_string(uint32_t ip_be) {
  const uint8_t a = uint8_t((ip_be >> 24) & 0xFF);
  const uint8_t b = uint8_t((ip_be >> 16) & 0xFF);
  const uint8_t c = uint8_t((ip_be >> 8) & 0xFF);
  const uint8_t d = uint8_t(ip_be & 0xFF);
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%u.%u.%u.%u", a, b, c, d);
  return std::string(buf);
}
// 读取 eth0 的 IPv4 和 netmask（返回 host-order u32）
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

// 从 /proc/net/route 读取默认网关（返回 host-order u32）
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
static bool apply_net_config_ioctl(const char* ifname, uint32_t ip_be, uint32_t mask_be, uint32_t gw_be,
                                   std::string& err) {
  int e = 0;

  // 1) set ip
  if (!set_ipv4_addr_be(ifname, ip_be, e)) {
    err = "SIOCSIFADDR failed errno=" + std::to_string(e);
    return false;
  }

  // 2) set netmask
  if (!set_ipv4_netmask_be(ifname, mask_be, e)) {
    err = "SIOCSIFNETMASK failed errno=" + std::to_string(e);
    return false;
  }

  // 3) set if up
  (void)set_if_up(ifname, e); // up 失败不一定致命

  // 4) replace default gateway
  // 先尝试删除旧默认路由（如果有）
  uint32_t old_gw_host = 0;
  if (get_default_gateway_host(ifname, old_gw_host)) {
    uint32_t old_gw_be = htonl(old_gw_host);
    int e2 = 0;
    (void)del_default_gateway_ioctl(ifname, old_gw_be, e2);
  }
  
  if (!add_default_gateway_ioctl(ifname, gw_be, e)) {
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

inline bool m5_write_persist(const M5PersistV1& in) {
  // 确保 /data 存在
  (void)ensure_dir("/data");
  int fd = ::open(kM5CfgPath, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return false;
  const size_t n = sizeof(M5PersistV1);
  const uint8_t* p = reinterpret_cast<const uint8_t*>(&in);
  size_t off = 0;
  while (off < n) {
    ssize_t w = ::write(fd, p + off, n - off);
    if (w < 0) { if (errno == EINTR) continue; ::close(fd); return false; }
    off += size_t(w);
  }
  ::close(fd);
  return true;
}

static constexpr uint16_t kM8DelayMaxMs = 500;
static constexpr uint16_t kM8VideoMaxDurMaxS = 15;
static constexpr uint16_t kM8TimeoutDefaultS = 60;

inline uint16_t u16_be(const uint8_t* p) {
  return uint16_t((uint16_t(p[0]) << 8) | uint16_t(p[1]));
}
inline void w16_be(uint8_t* p, uint16_t v) {
  p[0] = uint8_t((v >> 8) & 0xFF);
  p[1] = uint8_t(v & 0xFF);
}

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
    rt.video_bitrate_kbps.store(uint16_t(cfg.video_bitrate_unit) * 32u);

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

  // media 默认回落
  if (rt.m5_video_bitrate_unit.load() == 0) rt.m5_video_bitrate_unit.store(18);
  if (rt.video_bitrate_kbps.load() == 0) rt.video_bitrate_kbps.store(uint16_t(rt.m5_video_bitrate_unit.load()) * 32u);
  if (rt.m5_video_fps.load() == 0) rt.m5_video_fps.store(25);
  if (rt.m5_pic_quality.load() == 0) rt.m5_pic_quality.store(75);

  // periodic 默认关闭
  rt.m5_measure_periodic.store(false);
  if (rt.m5_measure_interval_min.load() == 0) rt.m5_measure_interval_min.store(30);
}

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

inline bool m5_persist_save(const RuntimeConfig& rt) {
  M5PersistV1 cfg{};
  m5_snapshot_runtime_to_persist(rt, cfg);
  return m5_write_persist(cfg);
}


// ---------- BCD 时间同步（用于认证/心跳应答）----------
inline int from_bcd(uint8_t b) { return ((b >> 4) & 0x0F) * 10 + (b & 0x0F); }
inline int from_bcd16(uint16_t b) {
  int d3 = (b >> 12) & 0x0F;
  int d2 = (b >> 8)  & 0x0F;
  int d1 = (b >> 4)  & 0x0F;
  int d0 = (b >> 0)  & 0x0F;
  return d3 * 1000 + d2 * 100 + d1 * 10 + d0;
}

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
inline btt::proto::Frame make_auth_req(const DefaultConfig& def, RuntimeConfig& rt) {
  btt::proto::Frame f;
  f.cmd = 0x00;
  f.level = 0x00; // 设备->上位机 请求
  f.seq = rt.seq.fetch_add(1);

  // 14B: SN(5)+VER(3)+MODEL(1)+HB(1)+RECONN(1)+REASON(1)+MODE(1)+POS(1)
  f.payload.resize(14);
  size_t o = 0;

  f.payload[o++] = def.sn_model;
  f.payload[o++] = def.sn_year;
  f.payload[o++] = def.sn_month;
  f.payload[o++] = uint8_t(def.sn_batch >> 8);
  f.payload[o++] = uint8_t(def.sn_batch & 0xFF);

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

inline void handle_heartbeat_resp(const btt::proto::Frame& fr, RuntimeConfig& rt) {
  if (fr.payload.size() < 8) {
    LOGW("HB RESP payload too short: %zu", fr.payload.size());
    return;
  }
  (void)sync_system_time_from_bcd8(&fr.payload[0]);
  rt.hb_miss.store(0);
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

inline void apply_devcfg_runtime(const std::array<uint8_t, kDevCfgLen>& c, RuntimeConfig& rt) {
  rt.switch_model.store(c[devcfg_off::kSwitchModel]);
  rt.hb_interval_s.store(c[devcfg_off::kHbIntervalS]);
  rt.business_timeout_s.store(c[devcfg_off::kBusinessTimeoutS]);
  rt.reconnect_interval_s.store(c[devcfg_off::kReconnectIntervalS]);
  rt.no_comm_reboot_s.store(c[devcfg_off::kNoCommRebootS]);
}

inline btt::proto::Frame make_resp_2b(uint8_t cmd, uint16_t seq, uint8_t result, uint8_t reason) {
  btt::proto::Frame r;
  r.cmd = cmd;
  r.level = 0x01; // 设备->上位机 应答
  r.seq = seq;
  r.payload = {result, reason};
  return r;
}

inline bool is_bool01(uint8_t v) { return v == 0x00 || v == 0x01; }

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

inline void handle_get_devcfg_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                  btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
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
  resp.payload.assign(c.begin(), c.end());

  LOGI("0x21 GET_DEVCFG RESP (seq=%u)", fr.seq);
  log_devcfg_fields(c);

  outbound.push(std::move(resp));
}

// ---------- M3：0x30(拍照测量) / 0x39(开始录像) / 0x3A(停止录像) ----------
// 说明：

// - 当前阶段：录像直接保存 H.264 裸流（不做 MP4 封装）

// 目录：/data/events/<event_id>/
static constexpr const char* kEventsDir = "/data/events";

// ---- 存储策略（/data 专用于事件）----
static constexpr uint64_t kEventBudgetBytes  = 47ull * 1024 * 1024; // 事件可用预算（留出安全余量）
static constexpr uint64_t kReserveBytes      = 5ull  * 1024 * 1024; // /data 保底余量
static constexpr uint64_t kMetaMaxBytes      = 200ull;              // meta.bin 上限
static constexpr uint64_t kJpgMaxBytes       = 50ull * 1024;        // 单张 JPG 上限（模型上界）
static constexpr uint32_t kVideoMaxMs        = 15u  * 1000;         // 单路录像最大 15s
static constexpr uint32_t kEventMaxMs        = 60u  * 1000;         // 事件最大 60s（目前仅做基础框架，不主动应答）

inline uint32_t clamp_bitrate_kbps(uint32_t kbps) { return (kbps > 1024u) ? 1024u : kbps; }

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
inline uint64_t est_video15s_bytes(uint32_t bitrate_kbps) {
  const uint32_t kbps = clamp_bitrate_kbps(bitrate_kbps);
  return (uint64_t)kbps * 15ull * 1024ull / 8ull;
}

// A 类：4 张照片
inline uint64_t est_event_A_bytes() {
  return kMetaMaxBytes + 4ull * kJpgMaxBytes;
}

// B 类：4 张照片 + 4 路 15s 视频
inline uint64_t est_event_B_bytes(uint32_t bitrate_kbps) {
  return kMetaMaxBytes + 4ull * kJpgMaxBytes + 4ull * est_video15s_bytes(bitrate_kbps);
}

// 前置声明（供存储准入/清理使用）
inline bool is_hex8_dirname(const char* s);
inline uint32_t parse_hex8_u32(const char* s);
inline bool rm_rf(const std::string& path);
inline void cleanup_event_tmp_tar(uint32_t event_id);

inline uint64_t fs_free_bytes(const char* path) {
  struct statvfs v{};
  if (::statvfs(path, &v) != 0) return 0;
  return (uint64_t)v.f_bavail * (uint64_t)v.f_frsize;
}

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

inline void build_bcdtime8_array(uint8_t out8[8]) {// 获取当前系统时间并转换为 BCD 格式存入 out8 数组
  std::vector<uint8_t> t8;
  btt::proto::build_bcdtime8(t8);
  if (t8.size() >= 8) std::memcpy(out8, t8.data(), 8);
  else std::memset(out8, 0, 8);
}

inline void append_u32_be(std::vector<uint8_t>& out, uint32_t v) {
  out.push_back(uint8_t((v >> 24) & 0xFF));
  out.push_back(uint8_t((v >> 16) & 0xFF));
  out.push_back(uint8_t((v >> 8) & 0xFF));
  out.push_back(uint8_t(v & 0xFF));
}

class RealTimeStreamer {
public:
  RealTimeStreamer() = default;
  ~RealTimeStreamer() { stop_and_join(false, 0); }

  void attach(RuntimeConfig& rt, btt::utils::BlockingQueue<btt::proto::Frame>& ctrl_out) {
    rt_ = &rt;
    ctrl_out_ = &ctrl_out;
  }

  bool active() const { return active_.load(); }
  uint8_t cam_id() const { return cam_id_.load(); }

  bool match_params(uint8_t cam_id, uint32_t ip_be, uint16_t port) const {
    return active_.load() && cam_id_.load() == cam_id && ip_be_.load() == ip_be && port_.load() == port;
  }

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

  void keepalive() {
    last_keepalive_ms_.store(now_ms_steady());
  }

  bool close_by_host(uint8_t cam_id) {
    if (!active_.load()) return false;
    if (cam_id_.load() != cam_id) return false;
    stop_and_join(false, 0);
    return true;
  }

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

  void stop_and_join(bool notify, uint8_t reason) {
    const bool join = true;
    stop_internal(notify, reason, join);
  }

private:
  struct StreamFrame {
    std::vector<uint8_t> data;
    bool is_i = false;
  };

  static void append_u16_be_local(std::vector<uint8_t>& out, uint16_t v) {
    out.push_back(uint8_t((v >> 8) & 0xFF));
    out.push_back(uint8_t(v & 0xFF));
  }

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

inline uint8_t cam_pos_mask4_from_cfg(RuntimeConfig& rt, uint8_t cam_id) {// 根据 cam_id 获取对应的摄像头位置掩码（低4位）
  std::array<uint8_t, kDevCfgLen> c{};
  if (rt.devcfg_valid.load()) {
    std::lock_guard<std::mutex> lk(rt.devcfg_mu);//加锁保护,防护锁使用方法！！！重要实例
    c = rt.devcfg_raw;
  } else {
    c = make_default_devcfg(rt);// 使用默认配置
  }

  struct Pair { uint8_t id; uint8_t pos; } cams[4] = {//结构体数组，今典使用方式 ，非常牛逼
    {c[devcfg_off::kCam1Id], c[devcfg_off::kCam1Pos]},
    {c[devcfg_off::kCam2Id], c[devcfg_off::kCam2Pos]},
    {c[devcfg_off::kCam3Id], c[devcfg_off::kCam3Pos]},
    {c[devcfg_off::kCam4Id], c[devcfg_off::kCam4Pos]},
  };
  for (const auto& it : cams) {//C++11 范围 for 循环 ，新方法
    if (it.id == cam_id) return uint8_t(it.pos & 0x0F);
  }
  return 0;
}

class MediaManager {
public:
  MediaManager() {//构造函数，初始化成员变量
    // 用时间做种子，避免重启后 file_id 从 1 开始导致重复
    const uint32_t seed = uint32_t(::time(nullptr));//获取当前时间作为种子，重点nullprt 比null更安全,尽量使用nullprt
    //seed ^ 常数作为 搅乱初始值，让它随机一下
    next_event_id_.store(seed ^ 0xA55A5AA5u);//时间随机种子异或一个常数，增加随机性，作为事件ID的初始值
    next_file_id_.store((seed << 16) ^ 0x5AA55AA5u);//时间左移16位再异或一个常数，作为文件ID的初始值
    for (auto& r : rec_) { r.fd = -1; }//便利初始化录像通道文件描述符为-1，表示未打开，c++11 最新用法 for((auto &（公式） it （遍历的成员对象）: rec_(遍历的对象))
    self_ptr() = this;//静态指针指向当前对象实例
  }

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

  void set_streamer(RealTimeStreamer* s) { streamer_ = s; }
  void set_video_max_ms(uint32_t ms) { video_max_ms_.store(ms ? ms : kVideoMaxMs); }

  // 当前录制事件（用于 0x3A 停止后写 meta / 0x3E 查询）
  bool current_event(uint32_t& event_id, std::string& dir, uint8_t& light_fill, uint8_t& laser) const {
    // std::lock_guard<std::mutex> lk(mu_);
    event_id = cur_event_id_;
    dir = cur_event_dir_;
    light_fill = cur_light_fill_;
    laser = cur_laser_;
    return (event_id != 0 && !dir.empty());
  }

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

  event_id = next_event_id_.fetch_add(1);
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

    const uint32_t file_id = next_file_id_.fetch_add(1);
    char path[256];
    std::snprintf(path, sizeof(path), "%s/%08X.jpg", dir.c_str(), file_id);

    int fd = ::open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) {
      LOGE("open(%s) failed errno=%d", path, errno);
      return false;
    }
    ssize_t w = ::write(fd, jpg.data(), jpg.size());
    ::close(fd);
    if (w != (ssize_t)jpg.size()) {
      LOGE("write(%s) short w=%zd expect=%zu errno=%d", path, w, jpg.size(), errno);
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
         cam_id, light_fill, laser, path, out.file_len, out.pos_mask, out.file_type);
    return true;
  }


  // M5: 应用视频参数（分辨率/帧率/码率）到 4 路编码通道
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

      const uint32_t file_id = next_file_id_.fetch_add(1);
      char path[256];
      std::snprintf(path, sizeof(path), "%s/%08X.jpg", dir.c_str(), file_id);

      int fd = ::open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
      if (fd < 0) {
        LOGE("open(%s) failed errno=%d", path, errno);
        return false;
      }
      ssize_t w = ::write(fd, jpg.data(), jpg.size());
      ::close(fd);
      if (w != (ssize_t)jpg.size()) {
        LOGE("write(%s) short w=%zd expect=%zu errno=%d", path, w, jpg.size(), errno);
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

      LOGI("MEAS SNAP OK: cam=%u path=%s len=%u pos_mask=0x%02X", cam_id, path, f.file_len, f.pos_mask);
    }
    return true;
  }

  // M8: 在既有事件目录下补拍 4 路 JPG（用于操岔/过车结束阶段）
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

      const uint32_t file_id = next_file_id_.fetch_add(1);
      char path[256];
      std::snprintf(path, sizeof(path), "%s/%08X.jpg", dir.c_str(), file_id);

      int fd = ::open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
      if (fd < 0) {
        LOGE("open(%s) failed errno=%d", path, errno);
        return false;
      }
      ssize_t w = ::write(fd, jpg.data(), jpg.size());
      ::close(fd);
      if (w != (ssize_t)jpg.size()) {
        LOGE("write(%s) short w=%zd expect=%zu errno=%d", path, w, jpg.size(), errno);
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

      LOGI("M8 SNAP OK: cam=%u path=%s len=%u pos_mask=0x%02X", cam_id, path, f.file_len, f.pos_mask);
    }
    return true;
  }

  // 0x39：开始录像（保存 h264 裸流）
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

      const uint32_t file_id = next_file_id_.fetch_add(1);
      char path[256];
      std::snprintf(path, sizeof(path), "%s/%08X.h264", dir.c_str(), file_id);
      int fd = ::open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
      if (fd < 0) {
        LOGE("open(%s) failed errno=%d", path, errno);
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
      std::snprintf(r.path, sizeof(r.path), "%s", path);

      LOGI("REC START: ch=%u path=%s pos_mask=0x%02X", ch, path, r.pos_mask4);
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

  static int stream_ch0(FRAME_INFO fi, char* buf, int len) { auto* s = self_ptr(); return s ? s->on_stream(0, fi, buf, len) : 0; }
  static int stream_ch1(FRAME_INFO fi, char* buf, int len) { auto* s = self_ptr(); return s ? s->on_stream(1, fi, buf, len) : 0; }
  static int stream_ch2(FRAME_INFO fi, char* buf, int len) { auto* s = self_ptr(); return s ? s->on_stream(2, fi, buf, len) : 0; }
  static int stream_ch3(FRAME_INFO fi, char* buf, int len) { auto* s = self_ptr(); return s ? s->on_stream(3, fi, buf, len) : 0; }

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


inline void append_ip_be(std::vector<uint8_t>& out, uint32_t ip_be) {
  append_u32_be(out, ip_be);
}

inline void handle_set_netparam_11_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                      btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  if (fr.payload.size() != 29) {
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
      std::string err;
      bool ok = apply_net_config_ioctl("eth0", dev_ip, mask, gw, err);

     
      if (!ok) {
        result = 0x01; reason = 0x02;
        LOGW("0x11 SET_NET: apply_net_config_ioctl failed: %s", err.c_str());
      } else {
        rt.m5_dev_ip_be.store(dev_ip);
        rt.m5_gateway_be.store(gw);
        rt.m5_netmask_be.store(mask);
        rt.m5_host_ip_be.store(host_ip);
        rt.m5_host_port.store(host_pt);
        rt.m5_upgrade_server_ip_be.store(upg_ip);
        rt.m5_debug_server_ip_be.store(dbg_ip);
        rt.m5_debug_server_port.store(dbg_pt);
        rt.m5_debug_proto.store(dbg_pr);

        (void)m5_persist_save(rt);

        // 让连接线程断链重连，切换到新的 host_ip/port
        rt.force_disconnect_reason.store(21);
        rt.force_disconnect.store(true);

        LOGI("0x11 SET_NET OK: dev=%s gw=%s mask=%s host=%s:%u",
             ip_be_to_string(dev_ip).c_str(), ip_be_to_string(gw).c_str(), ip_be_to_string(mask).c_str(),
             ip_be_to_string(host_ip).c_str(), host_pt);
      }
    }
  }

  btt::proto::Frame resp;
  resp.cmd = 0x11;
  resp.level = 0x01;
  resp.seq = fr.seq;
  resp.payload = {result, reason};
  outbound.push(std::move(resp));
}

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
        vid_br == 0) {
      result = 0x01; reason = 0x01;
    } else {
      rt.m5_pic_res.store(vid_res); // 图片分辨率同录像分辨率
      rt.m5_pic_quality.store(pic_q);
      rt.m5_video_res.store(vid_res);
      rt.m5_video_fps.store(vid_fps);
      rt.m5_video_bitrate_unit.store(vid_br);
      rt.video_bitrate_kbps.store(uint16_t(vid_br) * 32u);

      (void)m5_persist_save(rt);

      // 尝试即时生效（对录像编码参数）
      if (!media.apply_video_params_from_rt(rt)) {
        // 不强制失败：底层库可能不支持运行时切换
        LOGW("0x1A SET_MEDIA: apply_video_params failed, keep runtime only");
      }

      LOGI("0x1A SET_MEDIA OK: pic_res=%u q=%u vid_res=%u fps=%u br_unit=%u(%ukbps)",
           vid_res, pic_q, vid_res, vid_fps, vid_br, unsigned(vid_br) * 32u);
    }
  }

  outbound.push(make_resp_2b(0x1A, fr.seq, result, reason));
}
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

static int     code_to_hif_pic(uint8_t code);  // 可选：如果你也需要 0x1A 用

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

  // 默认回退值（防止读取失败时全 0）
  uint8_t pic_res_code = 3;   // 你可固定默认
  uint8_t pic_quality  = 75;  // 默认 75
  uint8_t vid_res_code = 3;   // 默认 720P
  uint8_t vid_fps      = 25;  // 默认 25
  uint8_t vid_br_unit  = 0xFF;// 默认未知/不支持时 FF，或按你定义

  // 1) 从 HiF 读取参数
  hif_media_param_t mp;  
  std::memset(&mp, 0, sizeof(mp));

  int rc = HiF_media_get_param(0,&mp);  
  if (rc != 0) {
    result = 0x01;
    reason = 0x01;  // 读取失败原因码你自定义
  } else {
    // 2) 从 mp 映射到协议字段
    // --- 图片分辨率：如果你按前面要求“无效化图片分辨率”，这里可以固定返回默认
    // pic_res_code = 3;

    // 如果你仍想反映真实值（即使 0x1A 忽略，也能用于观察当前底层状态）
    pic_res_code = hif_pic_to_code(mp.pic_size);      // 
    vid_res_code = hif_pic_to_code(mp.pic_size);    // 

    // 图片质量：如果 HiF 有 quality 字段就用，没有就保留默认/返回 runtime
    pic_quality  = rt.m5_pic_quality.load();                    

    vid_fps      = static_cast<uint8_t>(mp.frame_rate);     
    // 协议里 vid_br_unit 是 32K 的倍数单位：bitrateUnit(1B)
    // 如果 mp.bitrate 是 bps 或 kbps，需要换算：
    // 假设 mp.bitrate_kbps：单位 kbps
    // bitrateUnit = (mp.bitrate_kbps * 1024) / (32*1024) = mp.bitrate_kbps / 32
    if (mp.bit_rate > 0) {                      
      vid_br_unit = static_cast<uint8_t>(mp.bit_rate / 32);
    } else {
      vid_br_unit = 0xFF;
    }
  }
  printf("get_media_1B:  vid_res=%u, fps=%d, bit_rate=%d \n",
         mp.pic_size, mp.frame_rate, mp.bit_rate);

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
  append_u16_be(f.payload, 0xFFFF);
  append_u16_be(f.payload, 0xFFFF);
  append_u16_be(f.payload, 0xFFFF);
  append_u16_be(f.payload, 0xFFFF); // 保留 2B

  f.payload.push_back(uint8_t(files.size() & 0xFF));
  for (const auto& fi : files) append_fileinfo25(f.payload, fi);
  return f;
}

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

inline void append_u16_be(std::vector<uint8_t>& out, uint16_t v) {
  out.push_back(uint8_t((v >> 8) & 0xFF));
  out.push_back(uint8_t(v & 0xFF));
}

inline uint32_t read_u32_be(const uint8_t* p) {
  return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | uint32_t(p[3]);
}

inline uint16_t read_u16_be(const uint8_t* p) {
  return uint16_t((uint16_t(p[0]) << 8) | uint16_t(p[1]));
}

inline bool write_all_bytes(const std::string& path, const uint8_t* p, size_t n) {
  int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return false;
  size_t off = 0;
  while (off < n) {
    const ssize_t w = ::write(fd, p + off, n - off);
    if (w < 0) { if (errno == EINTR) continue; ::close(fd); return false; }
    off += (size_t)w;
  }
  ::close(fd);
  return true;
}

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
  return write_all_bytes(path, buf.data(), buf.size());
}

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
// 成功结束条件：发送最后一个 0x50(flag=2) -> 等待媒体通道返回 0x50 ACK(payload=2B) -> 关闭媒体 socket -> 删除 /tmp 下的 tar.gz

static constexpr size_t kEventUploadChunkBytes = 1024;   // 1KB
static constexpr int    kEventUploadMaxSeconds = 60;     // >60s 视为超时，发送 0x32
static constexpr int    kEventUploadAckTimeoutMs = 10000; // 等待最后 ACK 超时


inline std::string make_event_dir_path(uint32_t event_id) {//拼接目标目录
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%s/%08X", kEventsDir, event_id);
  return std::string(buf);
}


inline std::string make_event_tar_path(uint32_t event_id) {
  char buf[64];
  std::snprintf(buf, sizeof(buf), "/tmp/evt_%08X.tar.gz", event_id);
  return std::string(buf);
}

inline bool path_is_dir(const std::string& p) {//调用系统函数产看目录状态
  struct stat st{};
  if (::stat(p.c_str(), &st) != 0) return false;
  return S_ISDIR(st.st_mode);
}

inline bool file_exists(const std::string& p) {
  return ::access(p.c_str(), F_OK) == 0;
}

inline uint64_t file_size_bytes(const std::string& p) {//st_size (文件大小)、st_mode (文件类型和权限)、st_mtime (最后修改时间)。
  struct stat st{};
  if (::stat(p.c_str(), &st) != 0) return 0;
  return (uint64_t)st.st_size;
}

inline bool unlink_noerr(const std::string& p) {
  if (::unlink(p.c_str()) == 0) return true;
  if (errno == ENOENT) return true;
  return false;
}

inline int run_cmd_wait(const std::string& cmd) {
  int rc = ::system(cmd.c_str());
  if (rc == -1) return -1;
#ifdef WIFEXITED
  if (WIFEXITED(rc)) return WEXITSTATUS(rc);
#endif
  return rc;
}

// 生成 /tmp/evt_xxxxxxxx.tar.gz （压缩整个事件目录）
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
inline void cleanup_event_tmp_tar(uint32_t event_id) {
  const std::string tar = make_event_tar_path(event_id);
  (void)unlink_noerr(tar);
}

// 最后一包(flag=2)之后等待 0x50 ACK（payload=2B：result, reason）
inline bool wait_media_0x50_ack(btt::net::TcpClient& cli, btt::proto::StreamDecoder& dec,
                               std::atomic<bool>& stop_flag, int timeout_ms,
                               uint8_t& out_result, uint8_t& out_reason) {
  out_result = 0x01;
  out_reason = 0x01;

  const int64_t t0 = now_ms_steady();
  uint8_t buf[2048];

  while (!stop_flag.load()) {
    const int64_t now = now_ms_steady();
    if (now - t0 > timeout_ms) return false;

    // select 等待可读
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(cli.fd(), &rfds);

    timeval tv{};
    const int remain = timeout_ms - int(now - t0);
    tv.tv_sec = remain / 1000;
    tv.tv_usec = (remain % 1000) * 1000;

    int rc = ::select(cli.fd() + 1, &rfds, nullptr, nullptr, &tv);
    if (rc < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (rc == 0) continue;

    ssize_t n = cli.recv_some(buf, sizeof(buf));
    if (n <= 0) return false;

    // 兼容部分上位机实现：ACK 可能只回 2 字节(result, reason)，不带协议头
    if (n == 2 && !(buf[0] == btt::proto::kHead0 && buf[1] == btt::proto::kHead1)) {
      out_result = buf[0];
      out_reason = buf[1];
      return out_result == 0x00;
    }

    auto frames = dec.push(buf, (size_t)n);
    for (auto& fr : frames) {
      if (fr.cmd != 0x50) continue;
      if (fr.level != 0x01) continue;
      if (fr.payload.size() < 2) continue;
      out_result = fr.payload[0];
      out_reason = fr.payload[1];
      return out_result == 0x00;
    }
  }
  return false;
}

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
  uint32_t event_id = 0;
  uint32_t offset = 0;
  uint32_t ip_be = 0;
  uint16_t port = 0;
};

// 单路事件上传管理器（收到 0x31 后启动线程，通过媒体通道推送 0x50）
class EventUploader {
public:
  EventUploader() = default;//默认，不初始
  ~EventUploader() { stop_and_join(); }//结束

  bool busy() const { return active_.load(); }//私有变量

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
    active_.store(true);

    th_ = std::thread([this, r, &rt, &ctrl_out, &stop_flag]{
      this->thread_main(r, rt, ctrl_out, stop_flag);
    });
    return true;
  }

  // 由 0x33 调用：请求终止
  bool request_cancel(uint32_t key_id, uint32_t event_id) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!active_.load()) return false;
    if (cur_.key_id != key_id || cur_.event_id != event_id) return false;
    cancel_reason_.store(2); // 上位机终止
    cancel_.store(true);
    const int fd = media_fd_.load();
    if (fd >= 0) ::shutdown(fd, SHUT_RDWR); // 让 send/ACK wait 尽快退出
    return true;
  }

  // M8 抢占：指定结束原因码（0x30/0x50/0x51 等），并尽快打断媒体通道
  bool request_preempt(uint8_t end_reason) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!active_.load()) return false;
    cancel_reason_.store(end_reason);
    cancel_.store(true);
    const int fd = media_fd_.load();
    if (fd >= 0) ::shutdown(fd, SHUT_RDWR);
    return true;
  }

  void stop_and_join() {
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
  void finish() {
    std::lock_guard<std::mutex> lk(mu_);
    active_.store(false);
  }

  void thread_main(const EventUploadReq31& r, RuntimeConfig& rt,
                   btt::utils::BlockingQueue<btt::proto::Frame>& ctrl_out,
                   std::atomic<bool>& stop_flag) {
    const int64_t t0 = now_ms_steady();

    // 结束原因定义（与上位机未必完全一致，按需扩展）
    // 0:正常结束 1:超时 2:上位机终止 3:内部错误/IO 4:连接失败 5:压缩失败 6:参数错误
    uint8_t end_reason = 3;

    const std::string event_dir = make_event_dir_path(r.event_id);
    if (!path_is_dir(event_dir)) {//产看目录状态
      LOGW("M6 upload: event dir not found: %s", event_dir.c_str());
      end_reason = 6;
      ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
      finish();
      return;
    }

    // 1) 准备 tar.gz（续传：offset>0 且 tar 已存在则复用；否则重建）
    std::string tar_path = make_event_tar_path(r.event_id);//拼接压缩包目录
    bool need_build = (r.offset == 0) || !file_exists(tar_path);
    if (need_build) {
      if (!build_event_tar_gz(r.event_id, tar_path)) {
        end_reason = 5;
        ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
        finish();
        return;
      }
    }

    const uint64_t total = file_size_bytes(tar_path);
    if (total == 0 || (uint64_t)r.offset > total) {
      LOGW("M6 upload: bad offset=%u total=%llu", r.offset, (unsigned long long)total);
      end_reason = 6;
      ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
      finish();
      return;
    }

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
    LOGI("M6 upload: media connected to %s:%u", ip.c_str(), r.port);

    // 3) 发送 0x50 文件流
    int fd = ::open(tar_path.c_str(), O_RDONLY);//要c_str()?因为open函数需要C风格的字符串!
    if (fd < 0) {
      LOGW("M6 upload: open tar failed: %s errno=%d", tar_path.c_str(), errno);
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

    btt::proto::StreamDecoder ack_dec;
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
        break;
      }

      const int64_t now = now_ms_steady();
      if (now - t0 > (int64_t)kEventUploadMaxSeconds * 1000) { end_reason = 1; break; }

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
        append_u32_be(f.payload, r.event_id);
        f.payload.push_back(0xFF);
        f.payload.push_back(2); // end
        append_u16_be(f.payload, stream_seq);

        auto bytes = btt::proto::encode(f);
        if (bytes.empty() || !media.send_all(bytes.data(), bytes.size())) {
          end_reason = cancel_.load() ? (cancel_reason_.load() ? cancel_reason_.load() : 2) : 3;
          break;
        }

        uint8_t ack_r=0, ack_rs=0;
        if (wait_media_0x50_ack(media, ack_dec, stop_flag, kEventUploadAckTimeoutMs, ack_r, ack_rs)) {
          ok = true; end_reason = 0;
        } else {
          end_reason = cancel_.load() ? (cancel_reason_.load() ? cancel_reason_.load() : 2) : 3;
        }
        break;
      }

      pos += (uint64_t)n;
      const bool is_last = (pos >= total);

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
      append_u32_be(f.payload, r.event_id); // file_id=event_id（压缩包唯一标识）
      f.payload.push_back(0xFF);            // file_type 固定 FF
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
        uint8_t ack_r=0, ack_rs=0;
        if (wait_media_0x50_ack(media, ack_dec, stop_flag, kEventUploadAckTimeoutMs, ack_r, ack_rs)) {
          ok = true;
          end_reason = 0;
        } else {
          LOGW("M6 upload: wait ACK failed");
          end_reason = cancel_.load() ? (cancel_reason_.load() ? cancel_reason_.load() : 2) : 3;
        }
        break;
      }
    }

    ::close(fd);
    media.close_fd();
    media_fd_.store(-1);

    // 成功：删除 /tmp 下的 tar.gz
    if (ok || end_reason == 0x30 || end_reason == 0x50 || end_reason == 0x51) {
      cleanup_event_tmp_tar(r.event_id);
    }

    // 通知上位机结束（0x32）
    ctrl_out.push(make_event_upload_32(r.key_id, r.event_id, end_reason, rt));
    finish();
  }

  std::mutex mu_;
  std::thread th_;
  std::atomic<bool> active_{false};
  std::atomic<bool> cancel_{false};
  std::atomic<uint8_t> cancel_reason_{0};
  std::atomic<int> media_fd_{-1};
  EventUploadReq31 cur_{};
};

// 0x31 事件上传命令（上位机->设备）
inline void handle_event_upload_31_req(const btt::proto::Frame& fr, RuntimeConfig& rt, EventUploader& up,
                                      btt::utils::BlockingQueue<btt::proto::Frame>& outbound,
                                      std::atomic<bool>& stop_flag) {
  uint8_t result = 0x00;
  uint8_t reason = 0x00;

  EventUploadReq31 r{};
  if (fr.payload.size() != 18) {
    result = 0x01; reason = 0x01; // 参数错误
  } else {
    const uint8_t* p = fr.payload.data();
    r.key_id  = read_u32_be(p + 0);
    r.event_id = read_u32_be(p + 4);
    r.offset  = read_u32_be(p + 8);
    r.ip_be   = read_u32_be(p + 12);
    r.port    = read_u16_be(p + 16);

    if (r.port == 0) {
      result = 0x01; reason = 0x01;
    } else if (!path_is_dir(make_event_dir_path(r.event_id))) {
      result = 0x01; reason = 0x03; // not found
    } else if (up.busy()) {
      result = 0x01; reason = 0x02; // busy
    } else {
      // 启动上传线程
      if (!up.start(r, rt, outbound, stop_flag)) {
        result = 0x01; reason = 0x02;
      } else {
        LOGI("0x31 EVENT_UPLOAD start: key=%08X event=%08X offset=%u to %s:%u",
             r.key_id, r.event_id, r.offset, ip_be_to_string(r.ip_be).c_str(), r.port);
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

struct M8ActiveEvent {
  bool active = false;
  uint8_t event_type = 0xFF; // 0=操岔 1=过车
  uint32_t event_id = 0;
  std::string dir;
  uint8_t start_bcd[8]{};
  int64_t start_ms_steady = 0;
};

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

inline void handle_m8_event_52_resp(const btt::proto::Frame& fr) {
  if (fr.payload.size() < 2) return;
  const uint8_t result = fr.payload[0];
  const uint8_t reason = fr.payload[1];
  if (result == 0x00) LOGI("0x52 ACK: seq=%u result=%u reason=0x%02X", fr.seq, result, reason);
  else                LOGW("0x52 ACK FAIL: seq=%u result=%u reason=0x%02X", fr.seq, result, reason);
}

inline void handle_m8_event_53_resp(const btt::proto::Frame& fr) {
  if (fr.payload.size() < 2) return;
  const uint8_t result = fr.payload[0];
  const uint8_t reason = fr.payload[1];
  if (result == 0x00) LOGI("0x53 ACK: seq=%u result=%u reason=0x%02X", fr.seq, result, reason);
  else                LOGW("0x53 ACK FAIL: seq=%u result=%u reason=0x%02X", fr.seq, result, reason);
}

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

inline void handle_m8_strategy_29_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                      btt::utils::BlockingQueue<btt::proto::Frame>& outbound) {
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
      raw73[0] = event_type;
      raw73[24] = 0; // N=0，最短回显
    }
  } else {
    raw73[0] = 0xFF;
    raw73[24] = 0;
  }

  const uint8_t n = raw73[24];
  const uint8_t cnt = (n > 4) ? 4 : n;
  const size_t len = 25 + size_t(cnt) * 12;
  resp.payload.assign(raw73, raw73 + len);
  outbound.push(std::move(resp));
}

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

inline void handle_m8_switch_control_41_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                            MediaManager& media, RealTimeStreamer& streamer, EventUploader& uploader,
                                            M8ActiveEvent& st,
                                            std::atomic<uint32_t>& active_eid,
                                            std::atomic<uint8_t>& active_type,
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
    if (sd > 0) std::this_thread::sleep_for(milliseconds(sd));

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

  if (ed > 0) std::this_thread::sleep_for(milliseconds(ed));

  std::vector<FileInfo25> vids;
  (void)media.stop_record(0, true, 0, 0, vids);

  outbound.push(make_m8_switch_event_53(rt, 1, st.event_id, st.start_bcd, {}));

  if (picd > 0) std::this_thread::sleep_for(milliseconds(picd));

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

inline void handle_m8_pass_trigger_A6_req(const btt::proto::Frame& fr, RuntimeConfig& rt,
                                          MediaManager& media, RealTimeStreamer& streamer, EventUploader& uploader,
                                          M8ActiveEvent& st,
                                          std::atomic<uint32_t>& active_eid,
                                          std::atomic<uint8_t>& active_type,
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
    if (sd > 0) std::this_thread::sleep_for(milliseconds(sd));

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

  if (ed > 0) std::this_thread::sleep_for(milliseconds(ed));

  std::vector<FileInfo25> vids;
  (void)media.stop_record(0, true, 0, 0, vids);

  outbound.push(make_m8_pass_event_52(rt, 1, st.event_id, st.start_bcd, {}));

  if (picd > 0) std::this_thread::sleep_for(milliseconds(picd));

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

inline void handle_m8_internal_timeout(const btt::proto::Frame& fr, RuntimeConfig& rt, MediaManager& media,
                                      M8ActiveEvent& st,
                                      std::atomic<uint32_t>& active_eid,
                                      std::atomic<uint8_t>& active_type,
                                      std::atomic<int64_t>& deadline_ms,
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

// ----------------- 主运行：认证/心跳 + M2.1(0x20/0x21) + M3(0x30/0x39/0x3A) + M4(0x3D/0x3E/0x3F) + M5(0x11/0x12/0x1A/0x1B/0x38 + 0x5B) + M6(事件文件上传) + M7(0x35/0x36/0x37/0x51) + M8(0x1C/0x1D/0x28/0x29/0x41/0xA6/0x52/0x53) -----------------
inline int run_core(const DefaultConfig& def, RuntimeConfig& rt, std::atomic<bool>& stop_flag) {
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

  // M7：绑定实时流到媒体回调与控制通道
  streamer.attach(rt, outbound);
  media.set_streamer(&streamer);

  std::atomic<bool> stop{false};

  // init runtime
  rt.switch_model.store(def.switch_model);
  rt.last_rx_ms.store(now_ms_steady());
  rt.last_hb_tx_ms.store(now_ms_steady());
  

  auto disconnect = [&](uint8_t reason){
    LOGW("DISCONNECT reason=%u", reason);
    cli.close_fd();

  // M6：停止上传线程
  uploader.stop_and_join();
  // M7：停止实时流（不通知）
  streamer.stop_and_join(false, 0);
    rt.connected.store(false);
    rt.authed.store(false);
    rt.hb_miss.store(0);
    decoder.reset();
    rt.reconnect_reason.store(reason);
  };

  // NetRx
  std::thread t_rx([&]{
    uint8_t buf[2048];
    while (!stop.load()) {


      if (!rt.connected.load()) { std::this_thread::sleep_for(50ms); continue; }

      ssize_t n = cli.recv_some(buf, sizeof(buf));
      if (n > 0) {
        rt.last_rx_ms.store(now_ms_steady());
        rt.hb_miss.store(0);

        auto frames = decoder.push(buf, (size_t)n);
        for (auto& fr : frames) inbound.push(std::move(fr));
        continue;
      }

      if (n == 0) { disconnect(14); continue; }
      if (errno == EINTR) continue;
      if (errno == ECONNRESET) disconnect(15);
      else disconnect(16);
    }
  });

  // Actor
  std::thread t_actor([&]{
    while (!stop.load()) {


      auto opt = inbound.pop_wait(stop);
      if (!opt.has_value()) break;
      const auto& fr = *opt;



      // 开发期打印：优先看是否协议对齐（cmd/level/seq/len + raw）
      log_frame_detail("RX", fr);

      // 设备侧：收到0x00/0x01 一律按服务器应答处理
      if (fr.cmd == 0x00) {
        handle_auth_resp(fr, rt);
        if (!rt.authed.load()) disconnect(19);
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
      if (fr.cmd == 0x11) { handle_set_netparam_11_req(fr, rt, outbound); continue; }
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


      auto opt = outbound.pop_wait(stop);
      if (!opt.has_value()) break;
      if (!rt.connected.load()) continue;

      // 编码前打印
      log_frame_brief("TX", *opt);

      auto bytes = btt::proto::encode(*opt);
      if (bytes.empty()) continue;

      // 编码后 dump 整帧，方便与上位机/抓包对照
      log_hex("TX RAW", bytes.data(), bytes.size());

      if (!cli.send_all(bytes.data(), bytes.size())) {
        disconnect(16);
      }
    }
  });

  // Timer：连接/认证/心跳
  std::thread t_timer([&]{
    while (!stop.load()) {

      if (stop_flag.load()) { stop.store(true); inbound.notify_all(); outbound.notify_all(); break; }

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
        std::this_thread::sleep_for(200ms);
        continue;
      }

      if (rt.reconnect_disabled.load()) {
        std::this_thread::sleep_for(200ms);
        continue;
      }

      if (!rt.connected.load()) {
        const uint8_t ri = rt.reconnect_interval_s.load();
        if (ri == 0) { std::this_thread::sleep_for(200ms); continue; }

        {
        const uint32_t hip = rt.m5_host_ip_be.load();
        const uint16_t hpt = rt.m5_host_port.load();
        const std::string host_ip = ip_be_to_string(hip);
        LOGI("CONNECT to %s:%u ...", host_ip.c_str(), hpt);
        if (cli.connect_to(host_ip, hpt, 3000)) {
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
          LOGW("CONNECT FAIL, sleep %us", ri);
          std::this_thread::sleep_for(std::chrono::seconds(ri));
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
        std::this_thread::sleep_for(50ms);
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

      std::this_thread::sleep_for(50ms);
    }
  });



  // M5：周期测量线程（0x38 mode=0 配置后生效；整点对齐）
  std::thread t_measure([&]{
    while (!stop.load()) {
      if (stop_flag.load()) break;

      if (!rt.m5_measure_periodic.load()) {
        std::this_thread::sleep_for(200ms);
        continue;
      }

      const uint8_t interval = rt.m5_measure_interval_min.load();
      if (interval == 0) {
        std::this_thread::sleep_for(200ms);
        continue;
      }

      // 计算下一个“整点基线”触发点
      std::time_t now_t = ::time(nullptr);
      std::tm tm{};
      ::localtime_r(&now_t, &tm);

      int cur_min = tm.tm_min;
      int cur_sec = tm.tm_sec;
      int next_min = 0;
      if ((cur_min % interval) == 0 && cur_sec == 0) {
        next_min = cur_min; // 已在边界，立即触发
      } else {
        next_min = ((cur_min / interval) + 1) * interval;
        if (next_min >= 60) { next_min = 0; tm.tm_hour += 1; }
      }

      tm.tm_min = next_min;
      tm.tm_sec = 0;
      std::time_t next_t = ::mktime(&tm);
      if (next_t <= now_t) next_t = now_t + 1;

      // 睡眠直到 next_t（分段睡，便于 stop/配置变更）
      while (!stop.load()) {
        if (stop_flag.load()) break;
        if (!rt.m5_measure_periodic.load()) break;
        std::time_t tnow = ::time(nullptr);
        if (tnow >= next_t) break;
        std::this_thread::sleep_for(200ms);
      }
      if (stop.load() || stop_flag.load() || !rt.m5_measure_periodic.load()) continue;

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
    std::this_thread::sleep_for(100ms);
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

  return 0;
}

} // namespace btt::core
