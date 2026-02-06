// snap_only_ch0_cn.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include "HiF_media_ss522.h"

static volatile int g_exit = 0;

static void on_sigint(int sig) {
    (void)sig;
    printf("\n捕获到 Ctrl+C，准备退出...\n");
    g_exit = 1;
}

int main(void) {
    signal(SIGINT, on_sigint);

    printf("—— 仅抓拍与状态查询（通道0）——\n");

    if (HiF_media_init() != 0) {
        printf("错误：多媒体库初始化失败！\n");
        return -1;
    }
    printf("初始化成功。\n");

    int ch = 1;
    hif_media_param_t p;
    memset(&p, 0, sizeof(p));
    (void)HiF_media_get_param(ch, &p);   // 取默认值

    // 只配置通道0，且不注册回调（不录像、不取码流线程）
    p.pic_size   = HIF_PIC_D1_NTSC;                
    p.payload    = VE_TYPE_H264;
   

    if (HiF_media_set_param(ch, &p) != 0) {
        printf("错误：设置参数失败！\n");
        HiF_media_exit();
        return -1;
    }
    printf("参数设置完成。\n");

    if (HiF_media_start_encode() != 0) {
        printf("错误：启动编码失败！\n");
        HiF_media_exit();
        return -1;
    }
    printf("编码已启动，等待稳定...\n");
    sleep(1);

    printf("\n命令：s 抓拍   g 查询通道0状态   q 退出\n");

    unsigned char jpg_buf[1024 * 1024];  // 1080p 抓拍建议 1MB 缓冲
    char cmd[64];

    while (!g_exit) {
        printf("请输入命令：");
        if (!fgets(cmd, sizeof(cmd), stdin)) break;

        if (cmd[0] == 'q') {
            printf("收到退出命令。\n");
            break;
        } else if (cmd[0] == 'g') {
            hif_video_loss_t st = {0};
            st.ch = 0;
            int r = HiF_media_get_cam_status(&st);
            if (r == 0) {
                printf("通道0 状态：%s（is_lost=%d）\n",
                       st.is_lost ? "丢失" : "正常", st.is_lost);
            } else {
                printf("查询失败，返回码=%d\n", r);
            }
        } else if (cmd[0] == 's') {
            printf("正在抓拍...\n");
            int len = HiF_media_snap(ch,jpg_buf, sizeof(jpg_buf));
            if (len > 0) {
                FILE *fp = fopen("./snap.jpg", "wb");
                if (!fp) {
                    printf("错误：无法创建文件 ./snap.jpg\n");
                } else {
                    fwrite(jpg_buf, 1, len, fp);
                    fclose(fp);
                    printf("抓拍成功，大小 %d 字节，已保存 ./snap.jpg\n", len);
                }
            } else {
                printf("抓拍失败，返回码=%d\n", len);
            }
        } else {
            printf("无效命令。\n");
        }
    }

    HiF_media_exit();
    printf("程序已退出。\n");
    return 0;
}
