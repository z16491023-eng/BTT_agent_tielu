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
#include "libs/core/core_app.hpp"

static std::atomic<bool> g_stop{false};

static void on_sig(int) {
  g_stop.store(true);
}

namespace {

enum class ParseResult {
  kOk = 0,
  kHelp,
  kError,
};

static void print_usage(const char* prog) {
  std::fprintf(stderr,
               "Usage: %s [IP[:PORT] | IP PORT | PORT | --server=IP[:PORT]]\n"
               "Examples:\n"
               "  %s\n"
               "  %s 192.168.14.250:18502\n"
               "  %s 192.168.14.250 18502\n"
               "  %s 18502\n",
               prog, prog, prog, prog, prog);
}

static bool parse_port(const std::string& s, uint16_t& out) {
  if (s.empty()) return false;
  errno = 0;
  char* end = nullptr;
  const unsigned long v = std::strtoul(s.c_str(), &end, 10);
  if (errno != 0 || end == nullptr || *end != '\0' || v == 0 || v > 65535UL) return false;
  out = static_cast<uint16_t>(v);
  return true;
}

static bool is_ipv4(const std::string& ip) {
  in_addr a{};
  return ::inet_pton(AF_INET, ip.c_str(), &a) == 1;
}

// 解析格式：IP、PORT、IP:PORT、:PORT、IP:
static bool parse_endpoint_token(const std::string& token, std::string& io_ip, uint16_t& io_port) {
  if (token.empty()) return false;

  const std::size_t colon = token.find(':');
  if (colon == std::string::npos) {
    uint16_t p = 0;
    if (parse_port(token, p)) {
      io_port = p;
      return true;
    }
    if (is_ipv4(token)) {
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
    if (!is_ipv4(ip_part)) return false;
    io_ip = ip_part;
  }
  if (!port_part.empty()) {
    uint16_t p = 0;
    if (!parse_port(port_part, p)) return false;
    io_port = p;
  }
  return true;
}

static ParseResult parse_server_override(int argc, char** argv, btt::core::DefaultConfig& def) {
  if (argc <= 1) return ParseResult::kOk;

  std::string arg1 = argv[1];
  if (arg1 == "-h" || arg1 == "--help") {
    print_usage(argv[0]);
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
      print_usage(argv[0]);
      return ParseResult::kError;
    }
    ok = parse_endpoint_token(arg1.substr(kServerPrefix.size()), ip, port);
  } else if (argc == 2) {
    ok = parse_endpoint_token(arg1, ip, port);
  } else if (argc == 3) {
    if (!is_ipv4(arg1)) {
      std::fprintf(stderr, "Invalid server IP: %s\n", arg1.c_str());
      print_usage(argv[0]);
      return ParseResult::kError;
    }
    uint16_t p = 0;
    if (!parse_port(argv[2], p)) {
      std::fprintf(stderr, "Invalid server port: %s\n", argv[2]);
      print_usage(argv[0]);
      return ParseResult::kError;
    }
    ip = arg1;
    port = p;
    ok = true;
  } else {
    print_usage(argv[0]);
    return ParseResult::kError;
  }

  if (!ok) {
    std::fprintf(stderr, "Invalid server endpoint suffix: %s\n", argv[1]);
    print_usage(argv[0]);
    return ParseResult::kError;
  }

  def.server_ip = std::move(ip);
  def.server_port = port;
  return ParseResult::kOk;
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT,  on_sig);
  std::signal(SIGTERM, on_sig);

  btt::core::DefaultConfig def;
  const ParseResult parse_ret = parse_server_override(argc, argv, def);
  if (parse_ret == ParseResult::kHelp) return 0;
  if (parse_ret == ParseResult::kError) return 2;

  // 日志滚动：/appfs/log/app.log (1MB) + app.log.1.. (总数6个文件≈<=6MB)
  (void)btt::log::init_file_sink("/appfs/log", "app.log", 1024 * 1024, 6);

  LOGI("Boot server target: %s:%u", def.server_ip.c_str(), def.server_port);
  btt::core::RuntimeConfig rt;

  const int rc = btt::core::run_core(def, rt, g_stop);
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
