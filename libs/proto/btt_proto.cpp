/**
 * @file
 * @brief BTT 协议帧编解码与 BCD 时间辅助函数实现。
 *
 * Invariants:
 * - 所有编码结果都使用统一帧头和末尾双字节校验布局。
 * - 解码器在坏帧场景下只向前推进缓冲区，不会回退已判定无效的字节。
 * - BCD 时间编码始终按本地时间生成 8 字节数组，末尾两字节表示毫秒。
 */

#include "libs/proto/btt_proto.hpp"

#include <chrono>
#include <cstring>
#include <ctime>
#include <utility>

namespace btt::proto {
namespace {

/**
 * @brief 计算给定缓冲区的异或校验值。
 * @param p 输入字节指针。
 * @param n 字节数。
 * @return 返回异或校验结果。
 */
uint8_t xor_sum(const uint8_t* p, size_t n) {
  uint8_t x = 0;
  for (size_t i = 0; i < n; ++i) x ^= p[i];
  return x;
}

}  // namespace

/**
 * @brief 按大端读取 16 位无符号整数。
 * @param p 输入字节指针。
 * @return 返回读取结果。
 */
uint16_t be16(const uint8_t* p) {
  return (uint16_t(p[0]) << 8) | uint16_t(p[1]);
}

/**
 * @brief 按大端读取 32 位无符号整数。
 * @param p 输入字节指针。
 * @return 返回读取结果。
 */
uint32_t be32(const uint8_t* p) {
  return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) |
         (uint32_t(p[2]) << 8) | uint32_t(p[3]);
}

/**
 * @brief 按大端写入 32 位无符号整数。
 * @param p 输出字节指针。
 * @param v 输入数值。
 */
void wr_be32(uint8_t* p, uint32_t v) {
  p[0] = uint8_t((v >> 24) & 0xFF);
  p[1] = uint8_t((v >> 16) & 0xFF);
  p[2] = uint8_t((v >> 8) & 0xFF);
  p[3] = uint8_t(v & 0xFF);
}

/**
 * @brief 按大端写入 16 位无符号整数。
 * @param p 输出字节指针。
 * @param v 输入数值。
 */
void wr_be16(uint8_t* p, uint16_t v) {
  p[0] = uint8_t(v >> 8);
  p[1] = uint8_t(v & 0xFF);
}

/**
 * @brief 将协议帧编码为线协议字节流。
 * @param f 待编码帧。
 * @return 返回编码后的字节数组；若负载超限则返回空数组。
 */
std::vector<uint8_t> encode(const Frame& f) {
  if (f.payload.size() > kMaxPayload) return {};
  const uint16_t n = static_cast<uint16_t>(f.payload.size());

  std::vector<uint8_t> out;
  out.resize(2 + 1 + 1 + 2 + 2 + n + 2);

  size_t off = 0;
  out[off++] = kHead0;
  out[off++] = kHead1;
  out[off++] = f.cmd;
  out[off++] = f.level;
  wr_be16(&out[off], f.seq);
  off += 2;
  wr_be16(&out[off], n);
  off += 2;

  if (n != 0) {
    std::memcpy(&out[off], f.payload.data(), n);
    off += n;
  }

  out[off++] = 0x00;
  out[off++] = xor_sum(out.data(), out.size() - 2);
  return out;
}

/**
 * @brief 清空内部缓存。
 */
void StreamDecoder::reset() { buf_.clear(); }

/**
 * @brief 向解码器追加一段字节流并尝试解析完整帧。
 * @param data 输入字节指针。
 * @param len 输入字节数。
 * @return 返回本次解析出的所有完整协议帧。
 */
std::vector<Frame> StreamDecoder::push(const uint8_t* data, size_t len) {
  buf_.insert(buf_.end(), data, data + len);
  std::vector<Frame> out;

  while (true) {
    size_t i = 0;
    for (; i + 1 < buf_.size(); ++i) {
      if (buf_[i] == kHead0 && buf_[i + 1] == kHead1) break;
    }
    if (i > 0) buf_.erase(buf_.begin(), buf_.begin() + i);
    if (buf_.size() < kMinFrameNoPayload) break;

    const uint16_t n = be16(&buf_[6]);
    if (n > kMaxPayload) {
      buf_.erase(buf_.begin(), buf_.begin() + 2);
      continue;
    }

    const size_t total = kMinFrameNoPayload + n;
    if (buf_.size() < total) break;

    const uint8_t c0 = buf_[total - 2];
    const uint8_t c1 = buf_[total - 1];
    if (c0 != 0x00) {
      buf_.erase(buf_.begin(), buf_.begin() + 2);
      continue;
    }

    const uint8_t expect = xor_sum(buf_.data(), total - 2);
    if (c1 != expect) {
      buf_.erase(buf_.begin(), buf_.begin() + 2);
      continue;
    }

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

/**
 * @brief 将 0..99 的十进制数编码为 1 字节 BCD。
 * @param v 输入数值。
 * @return 返回 BCD 编码结果。
 */
uint8_t to_bcd(uint8_t v) {
  return uint8_t(((v / 10) << 4) | (v % 10));
}

/**
 * @brief 将 0..999 的十进制数编码为 2 字节 BCD。
 * @param v 输入数值。
 * @return 返回 BCD 编码结果。
 */
uint16_t to_bcd16(uint16_t v) {
  return uint16_t(((v / 1000) << 12) | (((v / 100) % 10) << 8) |
                  (((v / 10) % 10) << 4) | (v % 10));
}

/**
 * @brief 构造当前本地时间对应的 8 字节 BCD 时间数组。
 * @param out8 输出缓冲区。
 */
void build_bcdtime8(std::vector<uint8_t>& out8) {
  out8.resize(8);

  const auto now = std::chrono::system_clock::now();
  const auto ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          now.time_since_epoch()) %
      1000;

  std::time_t t = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  localtime_r(&t, &tm);

  out8[0] = to_bcd(uint8_t((tm.tm_year + 1900) - 2000));
  out8[1] = to_bcd(uint8_t(tm.tm_mon + 1));
  out8[2] = to_bcd(uint8_t(tm.tm_mday));
  out8[3] = to_bcd(uint8_t(tm.tm_hour));
  out8[4] = to_bcd(uint8_t(tm.tm_min));
  out8[5] = to_bcd(uint8_t(tm.tm_sec));

  const uint16_t bcd_ms = to_bcd16(uint16_t(ms.count()));
  out8[6] = uint8_t(bcd_ms >> 8);
  out8[7] = uint8_t(bcd_ms & 0xFF);
}

}  // namespace btt::proto
