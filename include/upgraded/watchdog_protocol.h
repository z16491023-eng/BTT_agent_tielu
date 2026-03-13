/**
 * @file
 * @brief `btt-core` 与 `upgraded` 之间的软件看门狗协议。
 *
 * 关键不变量：
 * - 该头文件必须自给自足，供应用侧和守护进程侧同时包含。
 * - 协议仅用于同机 Unix Domain Socket 通信，不承担跨平台字节序兼容职责。
 * - 一条心跳必须覆盖四个关键线程的推进年龄，守护进程不接受“只报活、不报线程”
 *   的简化心跳。
 */

#ifndef INCLUDE_UPGRADED_WATCHDOG_PROTOCOL_H_
#define INCLUDE_UPGRADED_WATCHDOG_PROTOCOL_H_

#include <cstddef>
#include <cstdint>

namespace upgraded {

inline constexpr char kWatchdogSockPath[] = "/appfs/upgrade/upgraded.sock";
inline constexpr uint16_t kWatchdogProtocolVersion = 1;
inline constexpr uint32_t kWatchdogHeartbeatIntervalMs = 500;
inline constexpr uint32_t kWatchdogHeartbeatTimeoutMs = 2000;
inline constexpr uint32_t kWatchdogHealthyConfirmMs = 30000;
inline constexpr uint32_t kWatchdogStartupGraceMs = 5000;
inline constexpr uint32_t kWatchdogLevel1WindowMs = 60000;
inline constexpr uint32_t kWatchdogLevel1Limit = 3;
inline constexpr uint32_t kWatchdogHardwareTimeoutSec = 10;

inline constexpr uint32_t kWatchdogFlagConnected = 1u << 0;
inline constexpr uint32_t kWatchdogFlagAuthed = 1u << 1;

inline constexpr uint32_t kWatchdogThreadNetRx = 1u << 0;
inline constexpr uint32_t kWatchdogThreadActor = 1u << 1;
inline constexpr uint32_t kWatchdogThreadNetTx = 1u << 2;
inline constexpr uint32_t kWatchdogThreadTimer = 1u << 3;
inline constexpr uint32_t kWatchdogThreadMaskRequired =
    kWatchdogThreadNetRx | kWatchdogThreadActor | kWatchdogThreadNetTx |
    kWatchdogThreadTimer;

inline constexpr uint32_t kWatchdogFailHeartbeatTimeout = 0x3001;
inline constexpr uint32_t kWatchdogFailThreadStall = 0x3002;

#pragma pack(push, 1)
/**
 * @brief 软件看门狗心跳包。
 */
struct WatchdogPingV1 {
  uint8_t magic[4];
  uint16_t version;
  uint16_t size;
  uint32_t seq;
  uint32_t pid;
  uint64_t monotonic_ms;
  uint32_t flags;
  uint32_t thread_bitmap;
  uint32_t net_rx_age_ms;
  uint32_t actor_age_ms;
  uint32_t net_tx_age_ms;
  uint32_t timer_age_ms;
};
#pragma pack(pop)

static_assert(sizeof(WatchdogPingV1) == 48u,
              "WatchdogPingV1 size must remain stable");

inline constexpr uint8_t kWatchdogMagic[4] = {'U', 'W', 'D', '1'};

/**
 * @brief 用于初始化心跳包固定头。
 * @param ping 待初始化的心跳结构。
 */
inline void InitWatchdogPing(WatchdogPingV1* ping) {
  ping->magic[0] = kWatchdogMagic[0];
  ping->magic[1] = kWatchdogMagic[1];
  ping->magic[2] = kWatchdogMagic[2];
  ping->magic[3] = kWatchdogMagic[3];
  ping->version = kWatchdogProtocolVersion;
  ping->size = static_cast<uint16_t>(sizeof(WatchdogPingV1));
}

/**
 * @brief 判断心跳包固定头是否合法。
 * @param ping 待校验的心跳包。
 * @return 合法时返回 `true`。
 */
inline bool IsValidWatchdogPingHeader(const WatchdogPingV1& ping) {
  return ping.magic[0] == kWatchdogMagic[0] &&
         ping.magic[1] == kWatchdogMagic[1] &&
         ping.magic[2] == kWatchdogMagic[2] &&
         ping.magic[3] == kWatchdogMagic[3] &&
         ping.version == kWatchdogProtocolVersion &&
         ping.size == sizeof(WatchdogPingV1);
}

}  // namespace upgraded

#endif  // INCLUDE_UPGRADED_WATCHDOG_PROTOCOL_H_
