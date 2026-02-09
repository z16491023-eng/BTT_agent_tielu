#pragma once
#include <cstdint>
#include <vector>
#include <cstring>
#include <chrono>
#include <ctime>

namespace btt::proto {

// 帧格式
// E6 FB | cmd(1) | level(1) | seq(2) | len(2) | payload(len) | chk0(1) | chk1(1)
// chk0: 固定 0x00
// chk1: 对“校验和2字节之前的所有字节”做 XOR（不含 chk0/chk1）
constexpr uint8_t kHead0 = 0xE6;
constexpr uint8_t kHead1 = 0xFB;
constexpr size_t  kMaxPayload = 1400;

inline uint16_t be16(const uint8_t* p) { return (uint16_t(p[0]) << 8) | uint16_t(p[1]); }
inline uint32_t be32(const uint8_t* p) {
  return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | uint32_t(p[3]);
}
inline void wr_be32(uint8_t* p, uint32_t v) {
  p[0] = uint8_t((v >> 24) & 0xFF);
  p[1] = uint8_t((v >> 16) & 0xFF);
  p[2] = uint8_t((v >> 8) & 0xFF);
  p[3] = uint8_t(v & 0xFF);
}

inline void wr_be16(uint8_t* p, uint16_t v) { p[0] = uint8_t(v >> 8); p[1] = uint8_t(v & 0xFF); }

inline uint8_t xor_sum(const uint8_t* p, size_t n) {
  uint8_t x = 0;
  for (size_t i = 0; i < n; ++i) x ^= p[i];
  return x;
}

struct Frame {
  uint8_t  cmd   = 0;
  uint8_t  level = 0x00; // 0x00 请求(普通)，0x01 应答(普通)
  uint16_t seq   = 1;
  std::vector<uint8_t> payload;

  // 仅用于开发阶段定位协议对齐
  std::vector<uint8_t> raw; // RX 时填充整帧原始字节
};

// 编码：E6 FB cmd level seq len payload chk0 chk1
inline std::vector<uint8_t> encode(const Frame& f) {
  if (f.payload.size() > kMaxPayload) return {};
  const uint16_t n = static_cast<uint16_t>(f.payload.size());

  // 2(head)+1(cmd)+1(level)+2(seq)+2(len)+n(payload)+2(chk)=10+n
  std::vector<uint8_t> out;
  out.resize(2 + 1 + 1 + 2 + 2 + n + 2);

  size_t off = 0;
  out[off++] = kHead0;
  out[off++] = kHead1;
  out[off++] = f.cmd;
  out[off++] = f.level;
  wr_be16(&out[off], f.seq); off += 2;
  wr_be16(&out[off], n);     off += 2;

  if (n) { std::memcpy(&out[off], f.payload.data(), n); off += n; }

  out[off++] = 0x00;                             // chk0
  out[off++] = xor_sum(out.data(), out.size() - 2); // chk1：XOR(不含 chk0/chk1)
  return out;
}

// 解码器：支持粘包拆包
class StreamDecoder {
public:
  void reset() { buf_.clear(); }

  std::vector<Frame> push(const uint8_t* data, size_t len) {
    buf_.insert(buf_.end(), data, data + len);
    std::vector<Frame> out;

    while (true) {
      // 找帧头
      size_t i = 0;
      for (; i + 1 < buf_.size(); ++i) {
        if (buf_[i] == kHead0 && buf_[i + 1] == kHead1) break;
      }
      if (i > 0) buf_.erase(buf_.begin(), buf_.begin() + i);
      if (buf_.size() < kMinFrameNoPayload) break;

      // len 偏移：head(2)+cmd(1)+level(1)+seq(2)=6 -> len 在 [6..7]
      const uint16_t n = be16(&buf_[6]);
      if (n > kMaxPayload) { buf_.erase(buf_.begin(), buf_.begin() + 2); continue; }

      const size_t total = kMinFrameNoPayload + n;
      if (buf_.size() < total) break;

      // 校验：倒数第2字节必须 00
      const uint8_t c0 = buf_[total - 2];
      const uint8_t c1 = buf_[total - 1];
      if (c0 != 0x00) { buf_.erase(buf_.begin(), buf_.begin() + 2); continue; }

      const uint8_t expect = xor_sum(buf_.data(), total - 2); // 不含 chk0/chk1
      if (c1 != expect) { buf_.erase(buf_.begin(), buf_.begin() + 2); continue; }

      Frame f;
      f.cmd = buf_[2];
      f.level = buf_[3];
      f.seq = be16(&buf_[4]);
      f.payload.assign(buf_.begin() + 8, buf_.begin() + 8 + n);
      f.raw.assign(buf_.begin(), buf_.begin() + total);

      out.push_back(std::move(f));
      buf_.erase(buf_.begin(), buf_.begin() + total);
    }
    return out;
  }

private:
  // 2(head)+1(cmd)+1(level)+2(seq)+2(len)+2(chk)=10
  static constexpr size_t kMinFrameNoPayload = 10;
  std::vector<uint8_t> buf_;
};

// ----------------- BCD time helpers -----------------
// 8字节 bcdtime：year,month,date,hour,minute,second,ms(2)
// year=26 表示 2026
inline uint8_t to_bcd(uint8_t v) { return uint8_t(((v / 10) << 4) | (v % 10)); }
inline uint16_t to_bcd16(uint16_t v) {
  // 0..999 -> BCD: 0x0XYZ
  return uint16_t(((v / 1000) << 12) | (((v / 100) % 10) << 8) | (((v / 10) % 10) << 4) | (v % 10));
}

inline void build_bcdtime8(std::vector<uint8_t>& out8) {
  out8.resize(8);
  using namespace std::chrono;

  auto now = system_clock::now();
  auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

  std::time_t t = system_clock::to_time_t(now);
  std::tm tm{};
  localtime_r(&t, &tm);

  out8[0] = to_bcd(uint8_t((tm.tm_year + 1900) - 2000));
  out8[1] = to_bcd(uint8_t(tm.tm_mon + 1));
  out8[2] = to_bcd(uint8_t(tm.tm_mday));
  out8[3] = to_bcd(uint8_t(tm.tm_hour));
  out8[4] = to_bcd(uint8_t(tm.tm_min));
  out8[5] = to_bcd(uint8_t(tm.tm_sec));
  uint16_t bcdms = to_bcd16(uint16_t(ms.count())); // 0..999
  out8[6] = uint8_t(bcdms >> 8);
  out8[7] = uint8_t(bcdms & 0xFF);
}

} // namespace btt::proto
