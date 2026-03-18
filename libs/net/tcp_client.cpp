/**
 * @file
 * @brief `TcpClient` 的实现。
 *
 * Invariants:
 * - 连接建立前后都只允许 `fd_` 指向当前唯一活跃套接字。
 * - 非阻塞连接探测只在 `connect_to()` 内部短暂使用，成功后会恢复为阻塞模式。
 * - 所有失败路径都会关闭当前候选套接字，避免实例残留半初始化连接。
 */

#include "libs/net/tcp_client.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

namespace btt::net {

/**
 * @brief 析构 `TcpClient` 对象并关闭底层套接字。
 */
TcpClient::~TcpClient() { close_fd(); }

/**
 * @brief 连接到指定 TCP 服务端。
 * @param ip 目标 IPv4 地址字符串。
 * @param port 目标端口号。
 * @param timeout_ms 连接超时时间，单位毫秒。
 * @return `true` 表示连接建立成功，`false` 表示连接失败。
 */
bool TcpClient::connect_to(const std::string& ip, uint16_t port, int timeout_ms) {
  close_fd();
  fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd_ < 0) return false;

  const int flags = ::fcntl(fd_, F_GETFL, 0);
  (void)::fcntl(fd_, F_SETFL, flags | O_NONBLOCK);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) != 1) {
    close_fd();
    return false;
  }

  int rc = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  if (rc == 0) {
    finalize_socket();
    return true;
  }
  if (errno != EINPROGRESS) {
    close_fd();
    return false;
  }

  fd_set write_fds;
  FD_ZERO(&write_fds);
  FD_SET(fd_, &write_fds);

  timeval tv{};
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;

  rc = ::select(fd_ + 1, nullptr, &write_fds, nullptr, &tv);
  if (rc <= 0) {
    close_fd();
    return false;
  }

  int err = 0;
  socklen_t len = sizeof(err);
  ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len);
  if (err != 0) {
    close_fd();
    return false;
  }

  finalize_socket();
  return true;
}

/**
 * @brief 从当前连接读取一段数据。
 * @param buf 读取缓冲区。
 * @param n 最多读取的字节数。
 * @return 返回 `recv()` 的原始结果；若当前未连接则返回 `-1`。
 */
ssize_t TcpClient::recv_some(uint8_t* buf, size_t n) {
  if (fd_ < 0) return -1;
  return ::recv(fd_, buf, n, 0);
}

/**
 * @brief 尝试发送给定缓冲区中的全部数据。
 * @param buf 待发送数据指针。
 * @param n 待发送字节数。
 * @return `true` 表示本次调用已发送完全部数据，`false` 表示发送失败或未能一次性完成。
 */
bool TcpClient::send_all(const uint8_t* buf, size_t n) {
  if (fd_ < 0) return false;
  size_t off = 0;
  while (off < n) {
    const ssize_t sent = ::send(fd_, buf + off, n - off, MSG_NOSIGNAL);
    if (sent > 0) {
      off += static_cast<size_t>(sent);
      continue;
    }
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) return false;
    return false;
  }
  return true;
}

/**
 * @brief 关闭当前底层套接字。
 */
void TcpClient::close_fd() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

/**
 * @brief 在连接建立后统一设置套接字属性。
 *
 * @note 该函数会恢复阻塞模式，并启用 `TCP_NODELAY`、`SO_KEEPALIVE` 与统一的收发超时。
 */
void TcpClient::finalize_socket() {
  const int flags = ::fcntl(fd_, F_GETFL, 0);
  (void)::fcntl(fd_, F_SETFL, flags & ~O_NONBLOCK);

  int one = 1;
  (void)::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  (void)::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));

  timeval tv{};
  tv.tv_sec = 0;
  tv.tv_usec = 200 * 1000;
  (void)::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  (void)::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

}  // namespace btt::net
