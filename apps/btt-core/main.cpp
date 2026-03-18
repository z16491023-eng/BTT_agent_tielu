/**
 * @file
 * @brief `btt_core` 进程入口。
 *
 * Invariants:
 * - 本文件只负责进程级职责：参数解析、信号转停、日志初始化，以及根据 `RunCore()` 返回码执行重启动作。
 * - 业务状态机、媒体控制和持久化路径全部下沉到 `libs/core/core_app.cpp`，避免入口文件承担协议语义。
 */

#include "libs/core/core_app.hpp"
#include "libs/utils/log.hpp"

#include <atomic>
#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/reboot.h>
#include <unistd.h>
#include <string>
#include <utility>

namespace {

std::atomic<bool> g_stop_requested{false};

enum class ParseResult {
  kOk = 0,
  kHelp,
  kError,
};

/**
 * @brief 处理进程终止信号。
 */
void OnSignal(int) {
  g_stop_requested.store(true);
}

/**
 * @brief 输出命令行帮助信息。
 * @param program 程序名。
 */
void PrintUsage(const char* program) {
  std::fprintf(stderr,
               "Usage: %s [IP[:PORT] | IP PORT | PORT | --server=IP[:PORT]]\n"
               "Examples:\n"
               "  %s\n"
               "  %s 192.168.14.250:18502\n"
               "  %s 192.168.14.250 18502\n"
               "  %s 18502\n",
               program, program, program, program, program);
}

/**
 * @brief 解析端口字符串。
 * @param text 输入文本。
 * @param port_out 解析得到的端口输出参数。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
bool ParsePort(const std::string& text, uint16_t& port_out) {
  if (text.empty()) return false;
  errno = 0;
  char* end = nullptr;
  const unsigned long value = std::strtoul(text.c_str(), &end, 10);
  if (errno != 0 || end == nullptr || *end != '\0' || value == 0 ||
      value > 65535UL) {
    return false;
  }
  port_out = static_cast<uint16_t>(value);
  return true;
}

/**
 * @brief 校验 IPv4 地址字符串。
 * @param ip 待校验的 IPv4 字符串。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
bool IsIpv4(const std::string& ip) {
  in_addr address{};
  return ::inet_pton(AF_INET, ip.c_str(), &address) == 1;
}

/**
 * @brief 解析服务端地址参数。
 * @param token 待解析的地址参数。
 * @param io_ip 输入输出 IP 字符串。
 * @param io_port 输入输出端口。
 * @return `true` 表示成功或条件成立，`false` 表示失败或条件不成立。
 */
bool ParseEndpointToken(const std::string& token, std::string& io_ip,
                        uint16_t& io_port) {
  if (token.empty()) return false;

  const std::size_t colon = token.find(':');
  if (colon == std::string::npos) {
    uint16_t parsed_port = 0;
    if (ParsePort(token, parsed_port)) {
      io_port = parsed_port;
      return true;
    }
    if (IsIpv4(token)) {
      io_ip = token;
      return true;
    }
    return false;
  }

  if (token.find(':', colon + 1) != std::string::npos) return false;
  const std::string ip_part = token.substr(0, colon);
  const std::string port_part = token.substr(colon + 1);
  if (ip_part.empty() && port_part.empty()) return false;

  if (!ip_part.empty()) {
    if (!IsIpv4(ip_part)) return false;
    io_ip = ip_part;
  }
  if (!port_part.empty()) {
    uint16_t parsed_port = 0;
    if (!ParsePort(port_part, parsed_port)) return false;
    io_port = parsed_port;
  }
  return true;
}

/**
 * @brief 解析命令行中的服务端覆盖配置。
 * @param argc 命令行参数个数。
 * @param argv 命令行参数数组。
 * @param def 启动默认配置。
 * @return 返回解析结果。
 */
ParseResult ParseServerOverride(int argc, char** argv,
                                btt::core::DefaultConfig& def) {
  if (argc <= 1) return ParseResult::kOk;

  std::string arg1 = argv[1];
  if (arg1 == "-h" || arg1 == "--help") {
    PrintUsage(argv[0]);
    return ParseResult::kHelp;
  }

  // 允许写成: ./btt_core +192.168.14.250:18502
  if (!arg1.empty() && arg1.front() == '+') arg1.erase(arg1.begin());

  std::string ip = def.server_ip;
  uint16_t port = def.server_port;
  bool ok = false;

  const std::string kServerPrefix = "--server=";
  if (arg1.compare(0, kServerPrefix.size(), kServerPrefix) == 0) {
    if (argc != 2) {
      PrintUsage(argv[0]);
      return ParseResult::kError;
    }
    ok = ParseEndpointToken(arg1.substr(kServerPrefix.size()), ip, port);
  } else if (argc == 2) {
    ok = ParseEndpointToken(arg1, ip, port);
  } else if (argc == 3) {
    if (!IsIpv4(arg1)) {
      std::fprintf(stderr, "Invalid server IP: %s\n", arg1.c_str());
      PrintUsage(argv[0]);
      return ParseResult::kError;
    }
    uint16_t parsed_port = 0;
    if (!ParsePort(argv[2], parsed_port)) {
      std::fprintf(stderr, "Invalid server port: %s\n", argv[2]);
      PrintUsage(argv[0]);
      return ParseResult::kError;
    }
    ip = arg1;
    port = parsed_port;
    ok = true;
  } else {
    PrintUsage(argv[0]);
    return ParseResult::kError;
  }

  if (!ok) {
    std::fprintf(stderr, "Invalid server endpoint suffix: %s\n", argv[1]);
    PrintUsage(argv[0]);
    return ParseResult::kError;
  }

  def.server_ip = std::move(ip);
  def.server_port = port;
  return ParseResult::kOk;
}

}  // namespace

/**
 * @brief 运行 `btt_core` 进程主入口。
 * @param argc 命令行参数个数。
 * @param argv 命令行参数数组。
 * @return 返回计算结果。
 */
int main(int argc, char** argv) {
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  btt::core::DefaultConfig def;
  const ParseResult parse_ret = ParseServerOverride(argc, argv, def);
  if (parse_ret == ParseResult::kHelp) return 0;
  if (parse_ret == ParseResult::kError) return 2;

  // 日志滚动：/appfs/log/app.log (1MB) + app.log.1.. (总数6个文件≈<=6MB)
  (void)btt::log::init_file_sink("/appfs/log", "app.log", 1024 * 1024, 6);

  LOGI("Boot server target: %s:%u", def.server_ip.c_str(), def.server_port);
  btt::core::RuntimeConfig rt;

  const int rc = btt::core::RunCore(def, rt, g_stop_requested);
  if (rc == btt::core::kRunCoreExitRestartProgram) {
    ::execv("/proc/self/exe", argv);
    std::fprintf(stderr, "execv restart failed: %s\n", std::strerror(errno));
    return 3;
  }

  if (rc == btt::core::kRunCoreExitRebootDevice) {
    ::sync();
    if (::reboot(RB_AUTOBOOT) != 0) {
      std::fprintf(stderr, "reboot failed: %s\n", std::strerror(errno));
      return 4;
    }
    return 0;
  }

  return rc;
}
