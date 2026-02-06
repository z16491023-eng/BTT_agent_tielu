#pragma once
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// 初始化底层库（供 RS485 用）；TTL 不依赖库，也可以直接用
int  serialmgr_init(void);

// 打开 TTL：/dev/ttyAMA4，POSIX+termios，8N1，无流控；baud 典型 115200
int  serialmgr_open_ttl(int baud);

// 打开 RS485：/dev/ttyAMA1，通过第三方库 SERIAL_TYPE_RS485
int  serialmgr_open_rs485(int baud);

// 打开 ble：/dev/ttyAMA2
int  serialmgr_open_ble(int baudrate);
// 根据 fd 自动分流到 TTL(POSIX) 或 RS485(库) 实现
int  serialmgr_write(int fd, const void* buf, size_t len);
int  serialmgr_read (int fd, void* buf, size_t len, int timeout_ms);
int  serialmgr_close(int fd);

#ifdef __cplusplus
}
#endif
