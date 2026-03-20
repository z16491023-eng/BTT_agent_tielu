#ifndef LIBS_NET_TCP_CLIENT_HPP_
#define LIBS_NET_TCP_CLIENT_HPP_

/**
 * @file
 * @brief 简单的 TCP 客户端封装声明。
 *
 * Invariants:
 * - 一个 `TcpClient` 实例在任意时刻只持有一个套接字文件描述符。
 * - `connect_to()` 会先关闭旧连接，再建立新连接，避免实例同时管理多条连接。
 * - 连接建立成功后，套接字会恢复为阻塞模式，并配置统一的收发超时与常用 TCP 选项。
 */

#include <cstddef>
#include <cstdint>
#include <string>
#include <sys/types.h>

namespace btt::net {

/**
 * @brief 轻量 TCP 客户端。
 *
 * @note 该类只封装连接建立、基础收发和文件描述符生命周期管理，不负责重连策略与协议编解码。
 */
class TcpClient {
 public:
  /**
   * @brief 默认构造 `TcpClient` 对象。
   */
  TcpClient() = default;

  /**
   * @brief 析构 `TcpClient` 对象并关闭底层套接字。
   */
  ~TcpClient();

  /**
   * @brief 连接到指定 TCP 服务端。
   * @param ip 目标 IPv4 地址字符串。
   * @param port 目标端口号。
   * @param timeout_ms 连接超时时间，单位毫秒。
   * @return `true` 表示连接建立成功，`false` 表示连接失败。
   */
  bool connect_to(const std::string& ip, uint16_t port, int timeout_ms = 3000);

  /**
   * @brief 从当前连接读取一段数据。
   * @param buf 读取缓冲区。
   * @param n 最多读取的字节数。
   * @return 返回 `recv()` 的原始结果；若当前未连接则返回 `-1`。
   */
  ssize_t recv_some(uint8_t* buf, size_t n);

  /**
   * @brief 尝试发送给定缓冲区中的全部数据。
   * @param buf 待发送数据指针。
   * @param n 待发送字节数。
   * @return `true` 表示本次调用已发送完全部数据，`false` 表示发送失败或未能一次性完成。
   */
  bool send_all(const uint8_t* buf, size_t n);

  /**
   * @brief 获取当前底层套接字文件描述符。
   * @return 返回当前文件描述符；未连接时返回负值。
   */
  int fd() const { return fd_; }

  /**
   * @brief 获取最近一次套接字操作失败时记录的错误码。
   * @return 返回最近一次失败的 `errno`/`SO_ERROR`；若尚无失败则返回 0。
   */
  int last_error() const { return last_error_; }

  /**
   * @brief 关闭当前底层套接字。
   */
  void close_fd();

 private:
  /**
   * @brief 在连接建立后统一设置套接字属性。
   *
   * @note 该函数会恢复阻塞模式，并启用 `TCP_NODELAY`、`SO_KEEPALIVE` 与统一的收发超时。
   */
  void finalize_socket();

  int fd_{-1};
  int last_error_{0};
};

}  // namespace btt::net

#endif  // LIBS_NET_TCP_CLIENT_HPP_
