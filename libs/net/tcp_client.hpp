#ifndef LIBS_NET_TCP_CLIENT_HPP_
#define LIBS_NET_TCP_CLIENT_HPP_

#include <string>
#include <cstdint>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

namespace btt::net {

class TcpClient {
 public:
  ~TcpClient() { close_fd(); }

  bool connect_to(const std::string& ip, uint16_t port, int timeout_ms = 3000) {
    close_fd();
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) return false;

    // 非阻塞 connect + 超时
    int flags = ::fcntl(fd_, F_GETFL, 0);
    ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) != 1) { close_fd(); return false; }

    int rc = ::connect(fd_, (sockaddr*)&addr, sizeof(addr));
    if (rc == 0) { finalize_socket(); return true; }
    if (errno != EINPROGRESS) { close_fd(); return false; }

    fd_set wfds; FD_ZERO(&wfds); FD_SET(fd_, &wfds);
    timeval tv{};
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    rc = ::select(fd_ + 1, nullptr, &wfds, nullptr, &tv);
    if (rc <= 0) { close_fd(); return false; }

    int err = 0; socklen_t len = sizeof(err);
    ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err != 0) { close_fd(); return false; }

    finalize_socket();
    return true;
  }

  ssize_t recv_some(uint8_t* buf, size_t n) {
    if (fd_ < 0) return -1;
    return ::recv(fd_, buf, n, 0);
  }

  bool send_all(const uint8_t* buf, size_t n) {
    if (fd_ < 0) return false;
    size_t off = 0;
    while (off < n) {
      ssize_t s = ::send(fd_, buf + off, n - off, 0);
      if (s > 0) { off += size_t(s); continue; }
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
      return false;
    }
    return true;
  }

  int fd() const { return fd_; }
  void close_fd() {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
  }

 private:
  void finalize_socket() {
    int flags = ::fcntl(fd_, F_GETFL, 0);
    ::fcntl(fd_, F_SETFL, flags & ~O_NONBLOCK);
    int one = 1;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    ::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));

    timeval tv{};
    tv.tv_sec = 0;
    tv.tv_usec = 200 * 1000;
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  }

  int fd_{-1};
};

}  // namespace btt::net

#endif  // LIBS_NET_TCP_CLIENT_HPP_
