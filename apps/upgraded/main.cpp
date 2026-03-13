/**
 * @file
 * @brief `upgraded` 可执行文件入口。
 *
 * 关键不变量：
 * - 本文件只负责进程入口，不承载守护逻辑和状态机细节。
 * - 真正的业务行为全部委托给 `upgraded::RunUpgradedDaemon()`，避免入口再次膨胀。
 */

#include "apps/upgraded/upgraded.h"

/**
 * @brief `upgraded` 守护进程入口。
 * @return 由 `upgraded::RunUpgradedDaemon()` 返回的进程退出码。
 */
int main(int argc, char* argv[]) { return upgraded::RunUpgradedDaemon(); }
