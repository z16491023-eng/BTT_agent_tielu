#define _GNU_SOURCE
#include "serial_mgr.h"
#include "hgs_misc.h"   // 仅 RS485 用到
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <termios.h>
#include <sys/select.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <stdio.h>

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(a) (int)(sizeof(a)/sizeof((a)[0]))
#endif

// --------- fd 注册表：记录每个 fd 属于哪种后端（TTL/RS485） ----------
typedef enum { BACKEND_NONE=0, BACKEND_TTL_POSIX=1, BACKEND_RS485_LIB=2,BACKEND_BLE_POSIX=3 } backend_t;
typedef struct { int fd; backend_t be; } fd_entry_t;

static fd_entry_t g_fdtab[16]; // 足够用了；需要可扩展成动态表·

static void tab_add(int fd, backend_t be){
    for (int i=0;i<ARRAY_SIZE(g_fdtab);i++){
        if (g_fdtab[i].fd<=0){ g_fdtab[i].fd=fd; g_fdtab[i].be=be; return; }
    }
}
static backend_t tab_get(int fd){
    for (int i=0;i<ARRAY_SIZE(g_fdtab);i++){
        if (g_fdtab[i].fd==fd) return g_fdtab[i].be;
    }
    return BACKEND_NONE;
}
static void tab_del(int fd){
    for (int i=0;i<ARRAY_SIZE(g_fdtab);i++){
        if (g_fdtab[i].fd==fd){ g_fdtab[i].fd=0; g_fdtab[i].be=BACKEND_NONE; return; }
    }
}

// --------- 工具：波特率映射 / 设置原始模式 8N1 ----------
static speed_t map_baud(int baud){
    switch(baud){
        case 9600:   return B9600;
        case 19200:  return B19200;
        case 38400:  return B38400;
        case 57600:  return B57600;
        case 115200: return B115200;
#ifdef B230400
        case 230400: return B230400;
#endif
#ifdef B460800
        case 460800: return B460800;
#endif
        default:     return 0; // 非常规速率下面再用 termios2（可选）
    }
}

static int set_raw_8n1_no_flow(int fd, int baud){
    struct termios tio;
    if (tcgetattr(fd, &tio) != 0) return -1;

    cfmakeraw(&tio);
    // 8N1
    tio.c_cflag &= ~CSIZE;//先清除CSIZE数据位的内容
    tio.c_cflag |= CS8; //设置数据位数
    tio.c_cflag &= ~PARENB; /* 不使用奇偶校验 */
    tio.c_cflag &= ~CSTOPB; //设置停止位为1位

    // 关闭硬件/软件流控
// #ifdef CRTSCTS
//     tio.c_cflag &= ~CRTSCTS;
// #endif
//     tio.c_iflag &= ~(IXON|IXOFF|IXANY);

    // 本地接收
    // tio.c_cflag |= (CLOCAL | CREAD);

    // 读超时交给 select，这里设非阻塞读参数
    tio.c_cc[VMIN]  = 0;
    tio.c_cc[VTIME] = 0;//VMIN=0, VTIME=0
// 不阻塞：缓冲区里有多少就返回多少；若没有立即返回 0。

    speed_t sp = map_baud(baud);
    if (sp!=0){
        cfsetispeed(&tio, sp);//设置串口输出波特率
        cfsetospeed(&tio, sp);//设置串口输入波特率
        if (tcsetattr(fd, TCSANOW, &tio) != 0) return -2;//立即生效
    }else{
        // 非常规波特率（如 921600）：
        return -5;

    }
    // 刷掉历史残留
    tcflush(fd, TCIOFLUSH);//丢弃设备的缓冲队列中的数据。
    return 0;
}

// --------- 公共初始化 ----------
int serialmgr_init(void){
    // RS485 依赖的第三方库初始化
    hgs_misc_init();
    return 0;
}

// --------- 打开 TTL（POSIX）：/dev/ttyAMA4 ----------
int serialmgr_open_ttl(int baud){
    const char* dev = getenv("TTL_DEV");

    if (!dev || !*dev) dev = "/dev/ttyAMA4";

    int fd = open(dev, O_RDWR | O_NOCTTY | O_NONBLOCK);//O_NOCTTY：打开一个 TTY 时，不要把它设为本进程的“控制终端”。O_NONBLOCK（同义：O_NDELAY）：非阻塞打开/读写。
    if (fd < 0) {
        perror("[serialmgr] open TTL");
        return -1;
    }
    // 关闭 O_NONBLOCK，读写都走阻塞/半阻塞，配合上层 select
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags!=-1) fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);

    if (set_raw_8n1_no_flow(fd, baud) != 0){
        close(fd);
        return -2;
    }
    tab_add(fd, BACKEND_TTL_POSIX);
    return fd;
}
// --------- 打开 BEL/dev/ttyAMA2 ----------
int serialmgr_open_ble(int baud){
    

    const char* dev = "/dev/ttyAMA2";

    int fd = open(dev, O_RDWR | O_NOCTTY | O_NONBLOCK);
    if (fd < 0) {
        perror("[serialmgr] open BEL");
        return -1;
    }
    // 关闭 O_NONBLOCK，读写都走阻塞/半阻塞，配合上层 select
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags!=-1) fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);

    if (set_raw_8n1_no_flow(fd, baud) != 0){
        close(fd);
        return -2;
    }
    tab_add(fd, BACKEND_BLE_POSIX);
    return fd;
}
// --------- 打开 RS485（第三方库）：/dev/ttyAMA1 ----------
int serialmgr_open_rs485(int baud){
    hgs_serial_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.type     = SERIAL_TYPE_RS485;   // 由库选择 /dev/ttyAMA1；若库支持自定义设备名可在这里设置
    cfg.baudrate = baud;
    cfg.databits = 8;
    cfg.stopbits = 1;
    cfg.parity   = 'N';

    int fd = hgs_serial_open(&cfg);
    if (fd <= 0) return -1;

    tab_add(fd, BACKEND_RS485_LIB);
    return fd;
}

// --------- 写：根据后端分流 ----------
static int write_all(int fd, const void* buf, size_t len){
    const unsigned char* p=(const unsigned char*)buf; size_t left=len;
    while (left>0){
        ssize_t n = write(fd, p, left);
        if (n<0){
            if (errno==EINTR) continue;
            return -1;
        }
        if (n==0) return -1;
        left -= (size_t)n; p += n;
    }
    return (int)len;
}

int serialmgr_write(int fd, const void* buf, size_t len){
    if (fd<=0 || !buf || len==0) return -1;
    backend_t be = tab_get(fd);
    if (be == BACKEND_TTL_POSIX||be == BACKEND_BLE_POSIX){
        return write_all(fd, buf, len);
    }else if (be == BACKEND_RS485_LIB){
        int n = hgs_serial_write(fd, (const unsigned char*)buf, (int)len);
        return (n<0)? -1 : n;
    }
    return -1;
}

// --------- 读：select + 对应后端读 ----------
int serialmgr_read(int fd, void* buf, size_t len, int timeout_ms){
    if (fd<=0 || !buf || len==0) return -1;

    fd_set rds; FD_ZERO(&rds); FD_SET(fd, &rds);
    struct timeval tv = { .tv_sec = timeout_ms/1000, .tv_usec = (timeout_ms%1000)*1000 };
    int r = select(fd+1, &rds, NULL, NULL, &tv);
    if (r==0) return 0;      // 超时
    if (r<0){
        if (errno==EINTR) return 0;
        return -1;
    }

    backend_t be = tab_get(fd);
    if (be == BACKEND_TTL_POSIX||be ==BACKEND_BLE_POSIX ){
        ssize_t n = read(fd, buf, len);
        if (n<0){
            if (errno==EINTR) return 0;
            return -1;
        }
        return (int)n;
    }else if (be == BACKEND_RS485_LIB){
        int n = hgs_serial_read(fd, (unsigned char*)buf, (int)len);
        return (n<0)? -1 : n;
    }
    return -1;
}

// --------- 关闭 ----------
int serialmgr_close(int fd){
    if (fd<=0) return -1;
    backend_t be = tab_get(fd);
    tab_del(fd);
    if (be == BACKEND_TTL_POSIX||be ==BACKEND_BLE_POSIX ){
        return close(fd);
    }else if (be == BACKEND_RS485_LIB){
        return hgs_serial_close(fd);
    }
    return -1;
}
