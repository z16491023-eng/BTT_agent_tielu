#pragma once
#include <cstdio>
#include <cstdarg>
#include <ctime>
#include <string>
#include <mutex>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <cerrno>
#include <cstring>

namespace btt::log {

enum class Level { TRACE, DEBUG, INFO, WARN, ERROR, FATAL };

inline const char* to_str(Level lv) {
  switch (lv) {
    case Level::TRACE: return "TRACE";
    case Level::DEBUG: return "DEBUG";
    case Level::INFO:  return "INFO";
    case Level::WARN:  return "WARN";
    case Level::ERROR: return "ERROR";
    case Level::FATAL: return "FATAL";
  }
  return "UNK";
}

// 0=TRACE 1=DEBUG 2=INFO 3=WARN 4=ERROR 5=FATAL
#ifndef BTT_LOG_MIN_LEVEL
#define BTT_LOG_MIN_LEVEL 2
#endif

inline bool enabled(Level lv) {
  const int v = (lv == Level::TRACE) ? 0 :
                (lv == Level::DEBUG) ? 1 :
                (lv == Level::INFO)  ? 2 :
                (lv == Level::WARN)  ? 3 :
                (lv == Level::ERROR) ? 4 : 5;
  return v >= BTT_LOG_MIN_LEVEL;
}

struct FileSink {
  bool enabled = false;
  std::string dir;
  std::string base;
  size_t max_bytes = 1024 * 1024; // 单文件上限
  int max_files_total = 6;        // 总文件数=base + (N-1) 个备份
  int fd = -1;
  size_t cur_size = 0;
  std::mutex mu;
};

inline FileSink& sink() {
  static FileSink s;
  return s;
}

inline bool ensure_dir_recursive(const std::string& dir) {
  if (dir.empty()) return false;
  struct stat st{};
  if (::stat(dir.c_str(), &st) == 0) return S_ISDIR(st.st_mode);

  // 递归创建父目录
  auto pos = dir.find_last_of('/');
  if (pos != std::string::npos && pos > 0) {
    if (!ensure_dir_recursive(dir.substr(0, pos))) return false;
  }

  if (::mkdir(dir.c_str(), 0755) == 0) return true;
  return (errno == EEXIST);
}

inline std::string join_path(const std::string& a, const std::string& b) {
  if (a.empty()) return b;
  if (!a.empty() && a.back() == '/') return a + b;
  return a + "/" + b;
}

inline std::string log_path_n(const FileSink& s, int idx) {
  // idx==0 -> base
  std::string p = join_path(s.dir, s.base);
  if (idx <= 0) return p;
  return p + "." + std::to_string(idx);
}

inline void close_fd_locked(FileSink& s) {
  if (s.fd >= 0) { ::close(s.fd); s.fd = -1; }
  s.cur_size = 0;
}

inline bool open_fd_locked(FileSink& s, bool trunc) {
  if (!s.enabled) return false;
  if (!ensure_dir_recursive(s.dir)) return false;

  const std::string p = log_path_n(s, 0);
  int flags = O_CREAT | O_WRONLY | O_APPEND;
  if (trunc) flags = O_CREAT | O_WRONLY | O_TRUNC;

  int fd = ::open(p.c_str(), flags, 0644);
  if (fd < 0) return false;
  s.fd = fd;

  struct stat st{};
  if (::fstat(fd, &st) == 0) s.cur_size = (size_t)st.st_size;
  else s.cur_size = 0;
  return true;
}

inline void rotate_locked(FileSink& s) {
  // 关闭当前文件
  close_fd_locked(s);

  // 删除最旧：base.(max_files_total-1)
  const int last = s.max_files_total - 1;
  if (last >= 1) {
    const std::string p_last = log_path_n(s, last);
    (void)::unlink(p_last.c_str());
  }

  // base.(i) -> base.(i+1)
  for (int i = last - 1; i >= 1; --i) {
    const std::string src = log_path_n(s, i);
    const std::string dst = log_path_n(s, i + 1);
    (void)::rename(src.c_str(), dst.c_str());
  }

  // base -> base.1
  if (last >= 1) {
    const std::string src = log_path_n(s, 0);
    const std::string dst = log_path_n(s, 1);
    (void)::rename(src.c_str(), dst.c_str());
  }

  // 新建 base
  (void)open_fd_locked(s, /*trunc=*/true);
}

inline bool init_file_sink(const char* dir,
                           const char* base = "app.log",
                           size_t max_bytes = 1024 * 1024,
                           int max_files_total = 6) {
  if (!dir || !base || max_files_total < 1) return false;
  FileSink& s = sink();
  std::lock_guard<std::mutex> lk(s.mu);
  s.dir = dir;
  s.base = base;
  s.max_bytes = max_bytes;
  s.max_files_total = max_files_total;
  s.enabled = true;
  close_fd_locked(s);
  return open_fd_locked(s, /*trunc=*/false);
}

inline void write_file_line(const char* line, size_t len) {
  FileSink& s = sink();
  if (!s.enabled) return;
  std::lock_guard<std::mutex> lk(s.mu);

  if (s.fd < 0) {
    if (!open_fd_locked(s, /*trunc=*/false)) return;
  }

  if (s.max_bytes > 0 && s.cur_size + len > s.max_bytes) {
    rotate_locked(s);
    if (s.fd < 0) return;
  }

  ssize_t w = ::write(s.fd, line, len);
  if (w > 0) s.cur_size += (size_t)w;
}

inline void vlog(Level lv, const char* fmt, va_list ap) {
  if (!enabled(lv)) return;

  std::time_t t = std::time(nullptr);
  std::tm tm{};
  localtime_r(&t, &tm);
  char ts[32];
  std::snprintf(ts, sizeof(ts), "%04d-%02d-%02d %02d:%02d:%02d",
                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                tm.tm_hour, tm.tm_min, tm.tm_sec);

  // format msg
  va_list ap2;
  va_copy(ap2, ap);
  char msg[1024];
  int n = std::vsnprintf(msg, sizeof(msg), fmt, ap2);
  va_end(ap2);

  std::string line;
  line.reserve(128 + (n > 0 ? (size_t)n : 0));
  line.append("[");
  line.append(ts);
  line.append("] [");
  line.append(to_str(lv));
  line.append("] ");

  if (n < 0) {
    line.append("(format_error)");
  } else if ((size_t)n < sizeof(msg)) {
    line.append(msg, (size_t)n);
  } else {
    // 需要更大缓冲
    std::vector<char> big((size_t)n + 1);
    std::vsnprintf(big.data(), big.size(), fmt, ap);
    line.append(big.data());
  }

  line.push_back('\n');

  // stderr
  std::fwrite(line.data(), 1, line.size(), stderr);

  // file
  write_file_line(line.data(), line.size());
}

inline void log(Level lv, const char* fmt, ...) {
  va_list ap; va_start(ap, fmt);
  vlog(lv, fmt, ap);
  va_end(ap);
}

} // namespace btt::log

#define LOGT(...) btt::log::log(btt::log::Level::TRACE, __VA_ARGS__)
#define LOGD(...) btt::log::log(btt::log::Level::DEBUG, __VA_ARGS__)
#define LOGI(...) btt::log::log(btt::log::Level::INFO,  __VA_ARGS__)
#define LOGW(...) btt::log::log(btt::log::Level::WARN,  __VA_ARGS__)
#define LOGE(...) btt::log::log(btt::log::Level::ERROR, __VA_ARGS__)
