#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include "hgs_misc.h"

int main(void) {
    hgs_misc_init(); // 必须先初始化

    hgs_serial_config_t cfg = {
        .type = SERIAL_TYPE_UART, // 蓝牙 UART
        .baudrate = 115200,
        .databits = 8,
        .stopbits = 1,
        .parity = 'N',
    };
    int fd = hgs_serial_open(&cfg);
    if (fd <= 0) { perror("hgs_serial_open"); return 1; }

   const char* cmds[] = { "AT", "AT+VER", "AT+ROLE?", "AT+NAME?", "AT+BAUD?" };
    for (size_t i=0;i<sizeof(cmds)/sizeof(cmds[0]);++i) {
        const char* c = cmds[i];
        hgs_serial_write(fd, (const uint8_t*)c, strlen(c));
        usleep(100*1000); // 100ms
        uint8_t buf[256];
        int n = hgs_serial_read(fd, buf, sizeof(buf));
        if (n > 0) {
            fwrite(buf, 1, n, stdout); // 可能会看到 “+OK... \r\n” 或 “+ERR=...”
        }
    }
    

    hgs_serial_close(fd);
    return 0;
}
