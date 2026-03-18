/**
 * @file
 * @brief 轻量日志输出与文件滚动工具实现。
 *
 * Invariants:
 * - 文件 sink 的全部可变状态都只在内部实现文件中维护，不向外暴露轮转细节。
 * - 轮转和写入始终在同一把互斥锁下串行执行，避免跨线程覆盖同一日志文件。
 * - 每条日志在写入文件前都已经带有时间戳、级别前缀和换行符。
 */

#include "libs/utils/log.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace btt::log {
namespace {

/**
 * @brief 文件日志 sink 状态。
 *
 * @note `fd`、`cur_size` 与滚动相关字段必须在持有 `mu` 时访问。
 */
struct FileSink {
  bool enabled = false;
  std::string dir;
  std::string base;
  size_t max_bytes = 1024 * 1024;
  int max_files_total = 6;
  int fd = -1;
  size_t cur_size = 0;
  std::mutex mu;
};

/**
 * @brief 获取进程级唯一文件日志 sink。
 * @return 返回全局文件 sink 引用。
 */
FileSink& sink() {
  static FileSink s;
  return s;
}

/**
 * @brief 将日志级别转换为内部整数级别。
 * @param lv 日志级别。
 * @return 返回从 `0` 到 `5` 的级别值。
 */
int level_value(Level lv) {
  switch (lv) {
    case Level::TRACE:
      return 0;
    case Level::DEBUG:
      return 1;
    case Level::INFO:
      return 2;
    case Level::WARN:
      return 3;
    case Level::ERROR:
      return 4;
    case Level::FATAL:
      return 5;
  }
  return 5;
}

/**
 * @brief 递归确保目录存在。
 * @param dir 目标目录路径。
 * @return `true` 表示目录已存在或创建成功，`false` 表示失败。
 */
bool ensure_dir_recursive(const std::string& dir) {
  if (dir.empty()) return false;

  struct stat st{};
  if (::stat(dir.c_str(), &st) == 0) return S_ISDIR(st.st_mode);

  const std::size_t pos = dir.find_last_of('/');
  if (pos != std::string::npos && pos > 0) {
    if (!ensure_dir_recursive(dir.substr(0, pos))) return false;
  }

  if (::mkdir(dir.c_str(), 0755) == 0) return true;
  return errno == EEXIST;
}

/**
 * @brief 拼接目录与文件名。
 * @param a 前半段路径。
 * @param b 后半段路径。
 * @return 返回拼接后的路径字符串。
 */
std::string join_path(const std::string& a, const std::string& b) {
  if (a.empty()) return b;
  if (a.back() == '/') return a + b;
  return a + "/" + b;
}

/**
 * @brief 获取指定轮转序号对应的日志文件路径。
 * @param s 文件 sink 状态。
 * @param idx 轮转序号；`0` 表示当前活动文件。
 * @return 返回对应日志文件路径。
 */
std::string log_path_n(const FileSink& s, int idx) {
  const std::string p = join_path(s.dir, s.base);
  if (idx <= 0) return p;
  return p + "." + std::to_string(idx);
}

/**
 * @brief 在持锁状态下关闭当前日志文件。
 * @param s 文件 sink 状态。
 */
void close_fd_locked(FileSink& s) {
  if (s.fd >= 0) {
    (void)::close(s.fd);
    s.fd = -1;
  }
  s.cur_size = 0;
}

/**
 * @brief 在持锁状态下打开当前活动日志文件。
 * @param s 文件 sink 状态。
 * @param trunc 是否以截断方式打开文件。
 * @return `true` 表示打开成功，`false` 表示失败。
 */
bool open_fd_locked(FileSink& s, bool trunc) {
  if (!s.enabled) return false;
  if (!ensure_dir_recursive(s.dir)) return false;

  const std::string p = log_path_n(s, 0);
  int flags = O_CREAT | O_WRONLY | O_APPEND;
  if (trunc) flags = O_CREAT | O_WRONLY | O_TRUNC;

  const int fd = ::open(p.c_str(), flags, 0644);
  if (fd < 0) return false;
  s.fd = fd;

  struct stat st{};
  if (::fstat(fd, &st) == 0) {
    s.cur_size = static_cast<size_t>(st.st_size);
  } else {
    s.cur_size = 0;
  }
  return true;
}

/**
 * @brief 在持锁状态下执行日志轮转。
 * @param s 文件 sink 状态。
 *
 * @note WHY: 轮转顺序必须先删最旧、再从高序号向低序号倒序重命名，
 *       否则会在同名覆盖过程中丢失较新的备份文件。
 */
void rotate_locked(FileSink& s) {
  close_fd_locked(s);

  const int last = s.max_files_total - 1;
  if (last >= 1) {
    const std::string p_last = log_path_n(s, last);
    (void)::unlink(p_last.c_str());
  }

  for (int i = last - 1; i >= 1; --i) {
    const std::string src = log_path_n(s, i);
    const std::string dst = log_path_n(s, i + 1);
    (void)::rename(src.c_str(), dst.c_str());
  }

  if (last >= 1) {
    const std::string src = log_path_n(s, 0);
    const std::string dst = log_path_n(s, 1);
    (void)::rename(src.c_str(), dst.c_str());
  }

  (void)open_fd_locked(s, true);
}

/**
 * @brief 将一行日志写入文件 sink。
 * @param line 日志内容。
 * @param len 日志长度。
 */
void write_file_line(const char* line, size_t len) {
  FileSink& s = sink();
  if (!s.enabled) return;
  std::lock_guard<std::mutex> lk(s.mu);

  if (s.fd < 0) {
    if (!open_fd_locked(s, false)) return;
  }

  if (s.max_bytes > 0 && s.cur_size + len > s.max_bytes) {
    rotate_locked(s);
    if (s.fd < 0) return;
  }

  const ssize_t written = ::write(s.fd, line, len);
  if (written > 0) s.cur_size += static_cast<size_t>(written);
}

}  // namespace

/**
 * @brief 将日志级别转换为可打印字符串。
 * @param lv 日志级别。
 * @return 返回级别字符串。
 */
const char* to_str(Level lv) {
  switch (lv) {
    case Level::TRACE:
      return "TRACE";
    case Level::DEBUG:
      return "DEBUG";
    case Level::INFO:
      return "INFO";
    case Level::WARN:
      return "WARN";
    case Level::ERROR:
      return "ERROR";
    case Level::FATAL:
      return "FATAL";
  }
  return "UNK";
}

/**
 * @brief 判断指定日志级别当前是否启用。
 * @param lv 日志级别。
 * @return `true` 表示该级别会被输出，`false` 表示该级别被过滤。
 */
bool enabled(Level lv) { return level_value(lv) >= BTT_LOG_MIN_LEVEL; }

/**
 * @brief 初始化文件日志 sink。
 * @param dir 日志目录路径。
 * @param base 日志基础文件名。
 * @param max_bytes 单个文件最大字节数。
 * @param max_files_total 日志文件总数上限。
 * @return `true` 表示初始化成功，`false` 表示失败。
 */
bool init_file_sink(const char* dir, const char* base, size_t max_bytes,
                    int max_files_total) {
  if (dir == nullptr || base == nullptr || max_files_total < 1) return false;

  FileSink& s = sink();
  std::lock_guard<std::mutex> lk(s.mu);
  s.dir = dir;
  s.base = base;
  s.max_bytes = max_bytes;
  s.max_files_total = max_files_total;
  s.enabled = true;

  close_fd_locked(s);
  return open_fd_locked(s, false);
}

/**
 * @brief 按 `va_list` 参数格式化并输出日志。
 * @param lv 日志级别。
 * @param fmt `printf` 风格格式串。
 * @param ap 可变参数列表。
 */
void vlog(Level lv, const char* fmt, va_list ap) {
  if (!enabled(lv)) return;

  std::time_t t = std::time(nullptr);
  std::tm tm{};
  localtime_r(&t, &tm);

  char ts[32];
  std::snprintf(ts, sizeof(ts), "%04d-%02d-%02d %02d:%02d:%02d",
                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
                tm.tm_min, tm.tm_sec);

  char msg[1024];
  va_list size_args;
  va_copy(size_args, ap);
  const int n = std::vsnprintf(msg, sizeof(msg), fmt, size_args);
  va_end(size_args);

  std::string line;
  line.reserve(128 + (n > 0 ? static_cast<size_t>(n) : 0));
  line.append("[");
  line.append(ts);
  line.append("] [");
  line.append(to_str(lv));
  line.append("] ");

  if (n < 0) {
    line.append("(format_error)");
  } else if (static_cast<size_t>(n) < sizeof(msg)) {
    line.append(msg, static_cast<size_t>(n));
  } else {
    std::vector<char> big(static_cast<size_t>(n) + 1);
    va_list full_args;
    va_copy(full_args, ap);
    (void)std::vsnprintf(big.data(), big.size(), fmt, full_args);
    va_end(full_args);
    line.append(big.data());
  }

  line.push_back('\n');

  (void)std::fwrite(line.data(), 1, line.size(), stderr);
  write_file_line(line.data(), line.size());
}

/**
 * @brief 输出一条日志。
 * @param lv 日志级别。
 * @param fmt `printf` 风格格式串。
 * @param ... 可变参数列表。
 */
void log(Level lv, const char* fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  vlog(lv, fmt, ap);
  va_end(ap);
}

}  // namespace btt::log
