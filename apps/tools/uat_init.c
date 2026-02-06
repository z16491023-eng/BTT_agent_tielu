// uart_rx_meter.c  —  纯 POSIX 串口读 + 每秒统计
#define _GNU_SOURCE
#include <termios.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <signal.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "hgs_misc.h"
static volatile int running = 1;
static void on_int(int s){ (void)s; running = 0; }

static speed_t baud_to_const(unsigned long b){
    switch(b){
        case 9600: return B9600;
        case 19200: return B19200;
        case 38400: return B38400;
        case 57600: return B57600;
        case 115200: return B115200;
#ifdef B230400
        case 230400: return B230400;
#endif
#ifdef B460800
        case 460800: return B460800;
#endif
#ifdef B921600
        case 921600: return B921600;
#endif
        default: return B115200;
    }
}

static int uart_open_config(const char* dev, unsigned long baud){
    int fd = open(dev, O_RDONLY | O_NOCTTY | O_NONBLOCK);
    if(fd < 0){ perror("open"); return -1; }

    struct termios tio;
    if(tcgetattr(fd, &tio) < 0){ perror("tcgetattr"); close(fd); return -1; }

    cfmakeraw(&tio);                 // 关掉行编辑/回显/翻译
    tio.c_cflag &= ~CRTSCTS;         // 关硬件流控（若没接RTS/CTS）
    tio.c_iflag &= ~(IXON|IXOFF|IXANY); // 关软件流控（XON/XOFF）
    tio.c_cflag = (tio.c_cflag & ~CSIZE) | CS8; // 8 数据位
    tio.c_cflag |= CLOCAL | CREAD;   // 本地连接 + 允许接收

    cfsetispeed(&tio, baud_to_const(baud));
    cfsetospeed(&tio, baud_to_const(baud));

    // 读行为：不阻塞在 read() 里，靠 poll() 唤醒
    tio.c_cc[VMIN]  = 0; // read() 立刻返回已有字节
    tio.c_cc[VTIME] = 0; // 不自带超时

    if(tcsetattr(fd, TCSANOW, &tio) < 0){ perror("tcsetattr"); close(fd); return -1; }
    tcflush(fd, TCIFLUSH);  // 清输入缓冲，避免历史数据干扰
    return fd;
}

int main(int argc, char** argv){
    const char* dev  = (argc > 1) ? argv[1] : "/dev/ttyAMA2";
    unsigned long baud = (argc > 2) ? strtoul(argv[2], NULL, 10) : 115200;
    hgs_misc_init(); // 必须先初始化
    int fd = uart_open_config(dev, baud);
    if(fd < 0) return 1;

    signal(SIGINT, on_int);

    uint8_t buf[46384];              // 大一点无妨，减 syscall 次数
    size_t total = 0, sec_bytes = 0;

    struct timeval t0, last, now;
    gettimeofday(&t0, NULL); last = t0;

    struct pollfd pfd = { .fd = fd, .events = POLLIN };

    while(running){
        int pr = poll(&pfd, 1, 1000);   // 最多等 1 秒
        if(pr > 0 && (pfd.revents & POLLIN)){
            for(;;){ // 把内核缓冲一次性读干净
                ssize_t n = read(fd, buf, sizeof(buf));
                if(n > 0){ total += (size_t)n; sec_bytes += (size_t)n; }
                else if(n == -1 && (errno == EAGAIN || errno == EINTR)) break;
                else if(n == 0) break;
                else { perror("read"); running = 0; break; }
            }
        }

        gettimeofday(&now, NULL);
        double dt = (now.tv_sec - last.tv_sec) + (now.tv_usec - last.tv_usec)/1e6;
        if(dt >= 1.0){
            double tsec = (now.tv_sec - t0.tv_sec) + (now.tv_usec - t0.tv_usec)/1e6;
            double kbps = (sec_bytes * 8.0) / (dt * 1000.0);
            printf("[RX] +%.2fs  inst=%.1f kbps  sec_bytes=%zu  total=%zu\n",
                   tsec, kbps, sec_bytes, total);
            fflush(stdout);
            sec_bytes = 0; last = now;
        }
    }
    close(fd);
    return 0;
}
