/**
 * @file
 * @brief `upgraded` 守护进程的公开入口声明。
 *
 * 关键不变量：
 * - 头文件只暴露最小可执行接口，不暴露 `state.bin` 的磁盘布局和内部状态机细节。
 * - 具体实现必须维持单实例运行、`state.bin` 自愈和由状态驱动启动决策这三个约束。
 */

#ifndef APPS_UPGRADED_UPGRADED_H_
#define APPS_UPGRADED_UPGRADED_H_

namespace upgraded {

/**
 * @brief 运行 `upgraded` 守护进程主逻辑。
 * @return 正常退出或因已有实例运行而退出时返回 `0`；初始化失败时返回非零值。
 */
int RunUpgradedDaemon();

}  // namespace upgraded

#endif  // APPS_UPGRADED_UPGRADED_H_
