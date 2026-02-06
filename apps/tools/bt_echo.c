// apps/tools/bt_tx_meter.c
#define _GNU_SOURCE 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include "hgs_misc.h"
#include <unistd.h> 
static int64_t now_ms(void){
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec*1000 + ts.tv_nsec/1000000;
}

static void at_send(int fd, const char *cmd){
    char line[128];
    hgs_serial_write(fd, (const uint8_t*)cmd, (int)strlen(cmd));
    // 读一小会儿把回显/结果吞掉，避免撑满模块缓冲
    int64_t end = now_ms() + 200;
    while (now_ms() < end){
        int n = hgs_serial_read(fd, (uint8_t*)line, sizeof(line));
        if (n > 0) write(STDOUT_FILENO, line, n);
        usleep(10*1000);
    }
}

int main(int argc, char **argv){
    // 用法：bt_tx_meter [秒=120] [baud=115200] [chunk=1000] [--at]
    int duration = (argc>1)? atoi(argv[1]): 120;
    int baud     = (argc>2)? atoi(argv[2]): 115200;
    int chunk    = (argc>3)? atoi(argv[3]): 1000;
    int do_at    = (argc>4 && strcmp(argv[4],"--at")==0);

    if (chunk < 1)  chunk = 1;
    if (chunk > 4096) chunk = 4096; // 单次写入上限

    hgs_misc_init();

    hgs_serial_config_t cfg = {
        .type     = SERIAL_TYPE_UART, // 海飞库里：蓝牙模块那路 UART
        .baudrate = baud,
        .databits = 8,
        .stopbits = 1,
        .parity   = 'N',
    };
    int fd = hgs_serial_open(&cfg);
    if (fd <= 0){ perror("hgs_serial_open"); return 1; }

    // 准备一段可重复的数据模式（比全 0 更利于抓包观察）
    static uint8_t buf[4096];
    for (int i=0;i<4096;i++) buf[i] = (uint8_t)('A' + (i%26));

    int64_t t0 = now_ms();
    int64_t t1 = t0;
    size_t total = 0, sec_bytes = 0;

    fprintf(stdout, "TX test start: dur=%ds baud=%d chunk=%d\n", duration, baud, chunk);
    fflush(stdout);

    while (now_ms() - t0 < (int64_t)duration*1000){
        int n = hgs_serial_write(fd, buf, chunk);
        if (n < 0){
            perror("hgs_serial_write");
            usleep(1000); // 稍微让出 CPU
            continue;
        }
        total    += (size_t)n;
        sec_bytes+= (size_t)n;

        // 每秒报告一次瞬时速率
        int64_t now = now_ms();
        if (now - t1 >= 1000){
            double kbps = (sec_bytes*8.0)/1000.0; // 每秒字节 -> kbps
            double elap = (now - t0)/1000.0;
            printf("[TX] +%.1fs inst=%.1f kbps  sec_bytes=%zu  total=%zu\n",
                   elap, kbps, sec_bytes, total);
            fflush(stdout);
            sec_bytes = 0;
            t1 = now;
        }

        // 若模块缓冲顶住，适度小睡防止热占（可视情况调大/去掉）
        // usleep(1000);
    }

    double dt = (now_ms()-t0)/1000.0;
    printf("TX done: total=%zu bytes in %.2fs  => avg=%.1f kbps\n",
           total, dt, total*8.0/1000.0/dt);

    hgs_serial_close(fd);
    return 0;
}
