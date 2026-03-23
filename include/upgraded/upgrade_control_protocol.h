/**
 * @file
 * @brief `btt-core` 与 `upgraded` 之间的本地升级控制协议。
 *
 * 关键不变量：
 * - 该协议只运行在同机 Unix Domain Socket 上，不承担跨平台字节序兼容职责。
 * - 守护进程是升级状态机和 `state.bin` 的唯一事实来源；应用侧只能通过控制命令请求提交、
 *   查询和清除升级结果，不能自行改写状态文件。
 * - `0x58` 只表达“升级最终结果”，因此需要 `report_pending/report_result/report_reason`
 *   这组字段把结果持久化到守护进程侧，避免网络断开时丢失结论。
 */

#ifndef INCLUDE_UPGRADED_UPGRADE_CONTROL_PROTOCOL_H_
#define INCLUDE_UPGRADED_UPGRADE_CONTROL_PROTOCOL_H_

#include <cstddef>
#include <cstdint>

namespace upgraded {

inline constexpr char kUpgradeControlSockPath[] =
    "/appfs/upgrade/upgraded_ctl.sock";
inline constexpr uint16_t kUpgradeControlProtocolVersion = 1;

inline constexpr uint16_t kUpgradeCtlCommandCommitPackage = 1;
inline constexpr uint16_t kUpgradeCtlCommandQueryReport = 2;
inline constexpr uint16_t kUpgradeCtlCommandClearReport = 3;

inline constexpr uint8_t kUpgradeCtlResultOk = 0;
inline constexpr uint8_t kUpgradeCtlResultFail = 1;

inline constexpr uint8_t kUpgradeCtlReasonNone = 0x00;
inline constexpr uint8_t kUpgradeCtlReasonParamInvalid = 0x01;
inline constexpr uint8_t kUpgradeCtlReasonBusy = 0x02;
inline constexpr uint8_t kUpgradeCtlReasonStateConflict = 0x03;
inline constexpr uint8_t kUpgradeCtlReasonSequenceInvalid = 0x04;
inline constexpr uint8_t kUpgradeCtlReasonWritePkgPartFailed = 0x05;
inline constexpr uint8_t kUpgradeCtlReasonCommitFailed = 0x06;

inline constexpr uint8_t kUpgradePhaseNone = 0;
inline constexpr uint8_t kUpgradePhasePkgReady = 1;
inline constexpr uint8_t kUpgradePhaseInstalling = 2;
inline constexpr uint8_t kUpgradePhaseTesting = 3;
inline constexpr uint8_t kUpgradePhaseResultPending = 4;

inline constexpr uint8_t kUpgradeReportResultSuccess = 0;
inline constexpr uint8_t kUpgradeReportResultFail = 1;

inline constexpr uint8_t kUpgradeReportReasonNone = 0x00;
inline constexpr uint8_t kUpgradeReportReasonStartFailed = 0x01;
inline constexpr uint8_t kUpgradeReportReasonWatchdogTimeout = 0x02;
inline constexpr uint8_t kUpgradeReportReasonThreadStall = 0x03;
inline constexpr uint8_t kUpgradeReportReasonRollback = 0x04;

#pragma pack(push, 1)
/**
 * @brief `btt-core -> upgraded` 的本地控制请求。
 *
 * @note `value0/value1` 的语义由 `command` 决定：
 * - `COMMIT_PACKAGE`：`value0` 为应用侧已落盘的 `pkg.part` 字节数。
 * - `QUERY_REPORT/CLEAR_REPORT`：当前未使用，需置零。
 */
struct UpgradeCtlRequestV1 {
  uint8_t magic[4];
  uint16_t version;
  uint16_t size;
  uint16_t command;
  uint16_t reserved0;
  uint32_t pid;
  uint32_t value0;
  uint32_t value1;
};

/**
 * @brief `upgraded -> btt-core` 的本地控制应答。
 */
struct UpgradeCtlResponseV1 {
  uint8_t magic[4];
  uint16_t version;
  uint16_t size;
  uint16_t command;
  uint8_t result;
  uint8_t reason;
  uint8_t report_pending;
  uint8_t report_result;
  uint8_t report_reason;
  uint8_t upgrade_phase;
  uint8_t target_part;
  uint8_t reserved0;
  uint16_t reserved1;
  uint32_t pkg_size;
};
#pragma pack(pop)

static_assert(sizeof(UpgradeCtlRequestV1) == 24u,
              "UpgradeCtlRequestV1 size must remain stable");
static_assert(sizeof(UpgradeCtlResponseV1) == 24u,
              "UpgradeCtlResponseV1 size must remain stable");

inline constexpr uint8_t kUpgradeCtlReqMagic[4] = {'U', 'C', 'T', '1'};
inline constexpr uint8_t kUpgradeCtlRespMagic[4] = {'U', 'C', 'R', '1'};

/**
 * @brief 初始化本地控制请求的固定头。
 * @param request 待初始化的请求结构。
 * @param command 请求命令。
 */
inline void InitUpgradeCtlRequest(UpgradeCtlRequestV1* request,
                                  uint16_t command) {
  request->magic[0] = kUpgradeCtlReqMagic[0];
  request->magic[1] = kUpgradeCtlReqMagic[1];
  request->magic[2] = kUpgradeCtlReqMagic[2];
  request->magic[3] = kUpgradeCtlReqMagic[3];
  request->version = kUpgradeControlProtocolVersion;
  request->size = static_cast<uint16_t>(sizeof(UpgradeCtlRequestV1));
  request->command = command;
  request->reserved0 = 0;
  request->pid = 0;
  request->value0 = 0;
  request->value1 = 0;
}

/**
 * @brief 初始化本地控制应答的固定头。
 * @param response 待初始化的应答结构。
 * @param command 当前应答对应的命令。
 */
inline void InitUpgradeCtlResponse(UpgradeCtlResponseV1* response,
                                   uint16_t command) {
  response->magic[0] = kUpgradeCtlRespMagic[0];
  response->magic[1] = kUpgradeCtlRespMagic[1];
  response->magic[2] = kUpgradeCtlRespMagic[2];
  response->magic[3] = kUpgradeCtlRespMagic[3];
  response->version = kUpgradeControlProtocolVersion;
  response->size = static_cast<uint16_t>(sizeof(UpgradeCtlResponseV1));
  response->command = command;
  response->result = kUpgradeCtlResultOk;
  response->reason = kUpgradeCtlReasonNone;
  response->report_pending = 0;
  response->report_result = kUpgradeReportResultSuccess;
  response->report_reason = kUpgradeReportReasonNone;
  response->upgrade_phase = kUpgradePhaseNone;
  response->target_part = 0;
  response->reserved0 = 0;
  response->reserved1 = 0;
  response->pkg_size = 0;
}

/**
 * @brief 校验本地控制请求固定头是否合法。
 * @param request 待校验请求。
 * @return 合法时返回 `true`。
 */
inline bool IsValidUpgradeCtlRequestHeader(const UpgradeCtlRequestV1& request) {
  return request.magic[0] == kUpgradeCtlReqMagic[0] &&
         request.magic[1] == kUpgradeCtlReqMagic[1] &&
         request.magic[2] == kUpgradeCtlReqMagic[2] &&
         request.magic[3] == kUpgradeCtlReqMagic[3] &&
         request.version == kUpgradeControlProtocolVersion &&
         request.size == sizeof(UpgradeCtlRequestV1);
}

/**
 * @brief 校验本地控制应答固定头是否合法。
 * @param response 待校验应答。
 * @return 合法时返回 `true`。
 */
inline bool IsValidUpgradeCtlResponseHeader(
    const UpgradeCtlResponseV1& response) {
  return response.magic[0] == kUpgradeCtlRespMagic[0] &&
         response.magic[1] == kUpgradeCtlRespMagic[1] &&
         response.magic[2] == kUpgradeCtlRespMagic[2] &&
         response.magic[3] == kUpgradeCtlRespMagic[3] &&
         response.version == kUpgradeControlProtocolVersion &&
         response.size == sizeof(UpgradeCtlResponseV1);
}

}  // namespace upgraded

#endif  // INCLUDE_UPGRADED_UPGRADE_CONTROL_PROTOCOL_H_
