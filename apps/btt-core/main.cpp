#include <atomic>
#include <csignal>
#include "libs/core/core_app.hpp"

static std::atomic<bool> g_stop{false};

static void on_sig(int) {
  g_stop.store(true);
}

int main() {
  std::signal(SIGINT,  on_sig);
  std::signal(SIGTERM, on_sig);

  // 日志滚动：/appfs/log/app.log (1MB) + app.log.1.. (总数6个文件≈<=6MB)
  (void)btt::log::init_file_sink("/appfs/log", "app.log", 1024 * 1024, 6);

  // 直接用默认配置启动（无配置文件）
  btt::core::DefaultConfig def;
  btt::core::RuntimeConfig rt;

  return btt::core::run_core(def, rt, g_stop);
}
