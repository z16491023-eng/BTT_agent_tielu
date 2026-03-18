#ifndef LIBS_UTILS_BLOCKING_QUEUE_HPP_
#define LIBS_UTILS_BLOCKING_QUEUE_HPP_

/**
 * @file
 * @brief 简单的阻塞队列模板。
 *
 * Invariants:
 * - 所有队列元素都由互斥锁保护，`push()` 与弹出操作之间不存在无锁共享写入。
 * - `stop` 标志仅用于控制等待行为，不会丢弃队列中已经存在的元素。
 * - 队列不限制容量，调用方需要自行控制生产速度与背压策略。
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>

namespace btt::utils {

/**
 * @brief 基于互斥锁和条件变量的阻塞队列。
 * @tparam T 队列元素类型。
 */
template <typename T>
class BlockingQueue {
 public:
  /**
   * @brief 向队列尾部压入一个元素。
   * @param v 待入队元素。
   */
  void push(T v) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      q_.push_back(std::move(v));
    }
    cv_.notify_one();
  }

  /**
   * @brief 阻塞等待一个元素出队。
   * @param stop 停止标志。
   * @return 若成功取到元素则返回该元素；若 `stop` 已置位且队列为空则返回空值。
   * @note 当 `stop=true` 但队列非空时，函数仍会优先弹出剩余元素。
   */
  std::optional<T> pop_wait(const std::atomic<bool>& stop) {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [&] { return !q_.empty() || stop.load(); });
    if (q_.empty()) return std::nullopt;
    T v = std::move(q_.front());
    q_.pop_front();
    return v;
  }

  /**
   * @brief 在超时时间内等待一个元素出队。
   * @tparam Rep 时间计数类型。
   * @tparam Period 时间周期类型。
   * @param stop 停止标志。
   * @param timeout 等待超时时间。
   * @return 若成功取到元素则返回该元素；若超时或 `stop` 置位且队列为空则返回空值。
   */
  template <typename Rep, typename Period>
  std::optional<T> pop_wait_for(
      const std::atomic<bool>& stop,
      const std::chrono::duration<Rep, Period>& timeout) {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait_for(lk, timeout, [&] { return !q_.empty() || stop.load(); });
    if (q_.empty()) return std::nullopt;
    T v = std::move(q_.front());
    q_.pop_front();
    return v;
  }

  /**
   * @brief 唤醒所有等待中的消费者。
   */
  void notify_all() { cv_.notify_all(); }

  /**
   * @brief 清空当前队列中的所有元素。
   */
  void clear() {
    std::lock_guard<std::mutex> lk(mu_);
    q_.clear();
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<T> q_;
};

}  // namespace btt::utils

#endif  // LIBS_UTILS_BLOCKING_QUEUE_HPP_
