#ifndef LIBS_CORE_CORE_APP_HPP_
#define LIBS_CORE_CORE_APP_HPP_

#include <array>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>

/**
 * @file
 * @brief btt_core 核心运行入口与公开运行期配置。
 *
 * Invariants:
 * - `RunCore()` 是 `btt_core` 唯一的业务主循环入口；进程级信号、日志初始化与重启语义由 `main()` 负责。
 * - `RuntimeConfig` 保存跨线程共享的运行态，但协议状态机的语义转移由实现文件中的 actor 线程串行推进。
 * - 持久化文件的断电一致性由实现文件负责，调用方只通过本头文件暴露的配置与返回码交互。
 */

namespace btt::core {

/** @brief 请求当前进程执行自重启时返回的退出码。 */
inline constexpr int kRunCoreExitRestartProgram = 64;

/** @brief 请求整机重启时返回的退出码。 */
inline constexpr int kRunCoreExitRebootDevice = 65;

/**
 * @brief btt_core 的启动默认配置。
 *
 * @note 该结构体表达“进程启动时可注入的缺省值”；运行中可被协议配置覆盖的项，会在
 *       `RuntimeConfig` 中维护独立副本。
 */
struct DefaultConfig {
  std::string server_ip = "192.168.14.250";
  uint16_t server_port = 18502;

  uint8_t sn_model = 0x00;
  uint8_t sn_year = 0x00;
  uint8_t sn_month = 0x00;
  uint16_t sn_batch = 0x0001;

  uint8_t ver_major = 0x00;
  uint8_t ver_minor = 0x00;
  uint8_t ver_tag = 0x01;

  uint8_t switch_model = 0x01;
  uint8_t run_mode = 0x00;

  int auth_timeout_ms = 5000;
};

/**
 * @brief btt_core 的共享运行态。
 *
 * @note 该结构体被多个线程共享，因此字段以原子量、互斥锁和固定尺寸缓冲区为主。
 *       调用方可以在启动前设置初值，但运行期间不应绕过 `RunCore()` 直接改写状态机语义。
 */
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

  std::atomic<uint8_t> position{3};
  std::atomic<uint8_t> reconnect_reason{0};

  std::atomic<int> hb_miss{0};
  std::atomic<int64_t> last_rx_ms{0};
  std::atomic<int64_t> last_hb_tx_ms{0};
  std::atomic<int64_t> last_auth_tx_ms{0};
  std::atomic<int64_t> wd_net_rx_ms{0};
  std::atomic<int64_t> wd_actor_ms{0};
  std::atomic<int64_t> wd_net_tx_ms{0};
  std::atomic<int64_t> wd_timer_ms{0};
  std::atomic<uint32_t> wd_ping_seq{1};

  std::atomic<uint16_t> video_bitrate_kbps{560};

  std::atomic<bool> devcfg_valid{false};
  mutable std::mutex devcfg_mu;
  std::array<uint8_t, 36> devcfg_raw{};

  std::atomic<uint32_t> m5_dev_ip_be{0};
  std::atomic<uint32_t> m5_gateway_be{0};
  std::atomic<uint32_t> m5_netmask_be{0};

  std::atomic<uint32_t> m5_host_ip_be{0};
  std::atomic<uint16_t> m5_host_port{0};

  std::atomic<uint32_t> m5_upgrade_server_ip_be{0};
  std::atomic<uint32_t> m5_debug_server_ip_be{0};
  std::atomic<uint16_t> m5_debug_server_port{0};
  std::atomic<uint8_t> m5_debug_proto{0};

  std::atomic<uint8_t> m5_pic_res{3};
  std::atomic<uint8_t> m5_pic_quality{75};
  std::atomic<uint8_t> m5_video_res{3};
  std::atomic<uint8_t> m5_video_fps{25};
  std::atomic<uint8_t> m5_video_bitrate_unit{17};

  std::atomic<bool> m5_measure_periodic{false};
  std::atomic<uint8_t> m5_measure_interval_min{30};

  std::atomic<bool> force_disconnect{false};
  std::atomic<uint8_t> force_disconnect_reason{0};

  std::atomic<uint8_t> m8_switch_trigger{1};
  std::atomic<uint8_t> m8_pass_trigger{1};

  mutable std::mutex m8_mu;
  std::array<uint8_t, 73> m8_strategy_raw0{};
  std::array<uint8_t, 73> m8_strategy_raw1{};

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

/**
 * @brief 运行 btt_core 主循环。
 * @param def 启动默认配置，只在主循环初始化阶段读取。
 * @param rt 共享运行态；主循环会持续更新其字段，并与网络、定时、媒体线程协作。
 * @param stop_flag 由进程级信号处理逻辑置位，用于请求主循环有序退出。
 * @return `0` 表示正常退出；`kRunCoreExitRestartProgram` 表示请求自重启；
 *         `kRunCoreExitRebootDevice` 表示请求整机重启；其余值表示错误退出码。
 * @note `rt` 必须在 `RunCore()` 调用期间保持有效，且不应被外部线程整体替换或销毁。
 */
int RunCore(const DefaultConfig& def, RuntimeConfig& rt,
            std::atomic<bool>& stop_flag);

}  // namespace btt::core

#endif  // LIBS_CORE_CORE_APP_HPP_
