#ifndef LIBS_UTILS_BLOCKING_QUEUE_HPP_
#define LIBS_UTILS_BLOCKING_QUEUE_HPP_

#include <mutex>
#include <condition_variable>
#include <deque>
#include <optional>
#include <atomic>
#include <chrono>

namespace btt::utils {

template <typename T>
class BlockingQueue {
public:
  void push(T v) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      q_.push_back(std::move(v));
    }
    cv_.notify_one();
  }

  // stop=true 时，如果队列为空则返回空；如果队列非空仍会把剩余任务弹出
  std::optional<T> pop_wait(const std::atomic<bool>& stop) {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [&]{ return !q_.empty() || stop.load(); });
    if (q_.empty()) return std::nullopt;
    T v = std::move(q_.front());
    q_.pop_front();
    return v;
  }

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

  void notify_all() { cv_.notify_all(); }

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
