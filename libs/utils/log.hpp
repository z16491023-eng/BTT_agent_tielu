#ifndef LIBS_UTILS_LOG_HPP_
#define LIBS_UTILS_LOG_HPP_

/**
 * @file
 * @brief 轻量日志输出与文件滚动工具声明。
 *
 * Invariants:
 * - 每条日志总是先格式化为单行字符串，再同步写入 `stderr`，并在文件 sink 启用时写入日志文件。
 * - 文件 sink 的打开、关闭、滚动和大小计数都在同一把互斥锁下完成，避免并发写入破坏轮转状态。
 * - 日志轮转策略固定为 `base` + `base.1..base.N`，其中 `base` 永远表示当前活动文件。
 */

#include <cstdarg>
#include <cstddef>

namespace btt::log {

/** @brief 日志级别枚举。 */
enum class Level { TRACE, DEBUG, INFO, WARN, ERROR, FATAL };

/**
 * @brief 将日志级别转换为可打印字符串。
 * @param lv 日志级别。
 * @return 返回级别字符串。
 */
const char* to_str(Level lv);

// 0=TRACE 1=DEBUG 2=INFO 3=WARN 4=ERROR 5=FATAL
#ifndef BTT_LOG_MIN_LEVEL
#define BTT_LOG_MIN_LEVEL 2
#endif

/**
 * @brief 判断指定日志级别当前是否启用。
 * @param lv 日志级别。
 * @return `true` 表示该级别会被输出，`false` 表示该级别被过滤。
 */
bool enabled(Level lv);

/**
 * @brief 初始化文件日志 sink。
 * @param dir 日志目录路径。
 * @param base 日志基础文件名。
 * @param max_bytes 单个文件最大字节数。
 * @param max_files_total 日志文件总数上限。
 * @return `true` 表示初始化成功，`false` 表示失败。
 */
bool init_file_sink(const char* dir, const char* base = "app.log",
                    size_t max_bytes = 1024 * 1024,
                    int max_files_total = 6);

/**
 * @brief 按 `va_list` 参数格式化并输出日志。
 * @param lv 日志级别。
 * @param fmt `printf` 风格格式串。
 * @param ap 可变参数列表。
 */
void vlog(Level lv, const char* fmt, va_list ap);

/**
 * @brief 输出一条日志。
 * @param lv 日志级别。
 * @param fmt `printf` 风格格式串。
 * @param ... 可变参数列表。
 */
void log(Level lv, const char* fmt, ...);

}  // namespace btt::log

#define LOGT(...) btt::log::log(btt::log::Level::TRACE, __VA_ARGS__)
#define LOGD(...) btt::log::log(btt::log::Level::DEBUG, __VA_ARGS__)
#define LOGI(...) btt::log::log(btt::log::Level::INFO, __VA_ARGS__)
#define LOGW(...) btt::log::log(btt::log::Level::WARN, __VA_ARGS__)
#define LOGE(...) btt::log::log(btt::log::Level::ERROR, __VA_ARGS__)

#endif  // LIBS_UTILS_LOG_HPP_
