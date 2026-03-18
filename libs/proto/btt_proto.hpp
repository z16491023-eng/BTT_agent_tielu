#ifndef LIBS_PROTO_BTT_PROTO_HPP_
#define LIBS_PROTO_BTT_PROTO_HPP_

/**
 * @file
 * @brief BTT 协议帧编解码与 BCD 时间辅助函数声明。
 *
 * Invariants:
 * - `encode()` 生成的帧固定遵循 `E6 FB | cmd | level | seq | len | payload | chk0 | chk1` 布局。
 * - `StreamDecoder` 只输出头、长度和校验都合法的完整帧；不完整尾包会保留在内部缓冲区中等待下次输入。
 * - 时间辅助函数统一使用 8 字节 BCD 编码，毫秒字段始终占用末尾 2 字节。
 */

#include <cstddef>
#include <cstdint>
#include <vector>

namespace btt::proto {

/** @brief 协议帧头第 1 字节固定值。 */
constexpr uint8_t kHead0 = 0xE6;

/** @brief 协议帧头第 2 字节固定值。 */
constexpr uint8_t kHead1 = 0xFB;

/** @brief 单帧最大负载长度。 */
constexpr size_t kMaxPayload = 1400;

/**
 * @brief 按大端读取 16 位无符号整数。
 * @param p 输入字节指针。
 * @return 返回读取结果。
 */
uint16_t be16(const uint8_t* p);

/**
 * @brief 按大端读取 32 位无符号整数。
 * @param p 输入字节指针。
 * @return 返回读取结果。
 */
uint32_t be32(const uint8_t* p);

/**
 * @brief 按大端写入 32 位无符号整数。
 * @param p 输出字节指针。
 * @param v 输入数值。
 */
void wr_be32(uint8_t* p, uint32_t v);

/**
 * @brief 按大端写入 16 位无符号整数。
 * @param p 输出字节指针。
 * @param v 输入数值。
 */
void wr_be16(uint8_t* p, uint16_t v);

/**
 * @brief 协议帧对象。
 *
 * @note `raw` 仅在接收路径中用于保留整帧原始字节，便于调试协议对齐问题。
 */
struct Frame {
  uint8_t cmd = 0;
  uint8_t level = 0x00;
  uint16_t seq = 1;
  std::vector<uint8_t> payload;
  std::vector<uint8_t> raw;
};

/**
 * @brief 将协议帧编码为线协议字节流。
 * @param f 待编码帧。
 * @return 返回编码后的字节数组；若负载超限则返回空数组。
 */
std::vector<uint8_t> encode(const Frame& f);

/**
 * @brief 协议流解码器。
 *
 * @note 该类支持粘包与拆包；坏帧会通过丢弃帧头前缀或当前候选帧头的方式继续同步。
 */
class StreamDecoder {
 public:
  /**
   * @brief 清空内部缓存。
   */
  void reset();

  /**
   * @brief 向解码器追加一段字节流并尝试解析完整帧。
   * @param data 输入字节指针。
   * @param len 输入字节数。
   * @return 返回本次解析出的所有完整协议帧。
   */
  std::vector<Frame> push(const uint8_t* data, size_t len);

 private:
  static constexpr size_t kMinFrameNoPayload = 10;
  std::vector<uint8_t> buf_;
};

/**
 * @brief 将 0..99 的十进制数编码为 1 字节 BCD。
 * @param v 输入数值。
 * @return 返回 BCD 编码结果。
 */
uint8_t to_bcd(uint8_t v);

/**
 * @brief 将 0..999 的十进制数编码为 2 字节 BCD。
 * @param v 输入数值。
 * @return 返回 BCD 编码结果。
 */
uint16_t to_bcd16(uint16_t v);

/**
 * @brief 构造当前本地时间对应的 8 字节 BCD 时间数组。
 * @param out8 输出缓冲区。
 */
void build_bcdtime8(std::vector<uint8_t>& out8);

}  // namespace btt::proto

#endif  // LIBS_PROTO_BTT_PROTO_HPP_
