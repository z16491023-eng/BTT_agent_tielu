// apps/camerad/rec_30s.c  —— 线程安全 + 调试版
#define _GNU_SOURCE 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>
#include "HiF_media_ss522.h"
#include <unistd.h>
static volatile sig_atomic_t g_stop = 0;
static volatile int g_closing = 0;       // ★ 退出中，回调不再写文件
static pthread_mutex_t g_io_mtx = PTHREAD_MUTEX_INITIALIZER;

static hif_mp4_handle g_mp4 = NULL;
static FILE *g_raw = NULL;

static int   g_secs = 30;
static int   g_ch = 0;
static int   g_dump_raw = 0;
static int   g_no_mp4 = 0;               // 仅落裸流
static int   g_seen_idr = 0;             // 见到 I 帧再写 MP4
static hif_payload_type g_codec = VE_TYPE_H264;

static int   g_verbose = 0;

#define DEBUG(fmt, ...) do{ if(g_verbose){ fprintf(stderr,"[rec] " fmt "\n", ##__VA_ARGS__); } }while(0)

static unsigned long now_ms(void){
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (unsigned long)(ts.tv_sec*1000ul + ts.tv_nsec/1000000ul);
}

static unsigned long g_t0_ms = 0;

static void on_sigint(int s){ (void)s; g_stop = 1; }

static int on_stream(FRAME_INFO fi, char* buf, int len) {
    if (!buf || len <= 0) return 0;

    if (fi.frame_type == FRAME_I) g_seen_idr = 1;

    // ★ 退出中直接丢帧，防止使用已关闭句柄
    if (g_closing) return 0;

    pthread_mutex_lock(&g_io_mtx);

    // MP4：仅见到 I 帧后写入
    if (!g_no_mp4 && g_mp4 && g_seen_idr) {
        int w = HiF_media_mp4_write_frame(g_mp4, (unsigned char*)buf, len, (int)fi.timestamp);
        DEBUG("mp4 write: type=%d len=%d pts=%d ret=%d", fi.frame_type, len, (int)fi.timestamp, w);
    }
    // 裸流（可选）
    if (g_dump_raw && g_raw) {
        size_t w = fwrite(buf, 1, (size_t)len, g_raw);
        (void)w;
    }

    pthread_mutex_unlock(&g_io_mtx);

    if (now_ms() - g_t0_ms >= (unsigned long)g_secs*1000ul) g_stop = 1;
    return 0;
}

int main(int argc, char** argv) {
    if (argc > 1) g_codec = (strcmp(argv[1],"h265")==0) ? VE_TYPE_H265 : VE_TYPE_H264;
    if (argc > 2) g_secs = atoi(argv[2]);
    if (g_secs <= 0 || g_secs > 600) g_secs = 30;

    g_dump_raw = getenv("DUMP_RAW") ? 1 : 0;
    g_no_mp4   = getenv("NO_MP4")   ? 1 : 0;
    g_verbose  = getenv("VERBOSE")  ? 1 : 0;

    signal(SIGINT, on_sigint);

    if (HiF_media_init() != 0) { fprintf(stderr,"media init failed\n"); return 1; }

    // 可选：检查通道状态
    hif_video_loss_t st = { .ch = (unsigned char)g_ch };
    if (HiF_media_get_cam_status(&st) == 0 && st.is_lost) {
        fprintf(stderr, "WARN: ch%d video lost\n", g_ch);
    }

    // 编码参数（显式写关键域）
    hif_media_param_t p; memset(&p, 0, sizeof(p));
    (void)HiF_media_get_param(g_ch, &p);  // 拉默认，再覆盖
    p.pic_size   = HIF_PIC_D1_NTSC;
    p.payload    = g_codec;
    p.gop_mode   = VENC_GOP_MODE_NORMAL_P;
    p.rc_mode    = VENC_RC_CBR;                   // 0=CBR
    p.frame_rate = 25;
    p.bit_rate   = 4*1024;                        // kbps
    p.max_bit_rate = p.bit_rate;
    p.long_term_min_bit_rate = p.bit_rate/2;
    p.cb_get_stream = on_stream;

    if (HiF_media_set_param(g_ch, &p) != 0) {
        fprintf(stderr, "set_param failed\n"); HiF_media_exit(); return 2;
    }

    char path_mp4[256]; snprintf(path_mp4,sizeof(path_mp4), "./rec_%s_%ds.mp4",
        (g_codec==VE_TYPE_H265)?"h265":"h264", g_secs);
    char path_raw[256]; snprintf(path_raw,sizeof(path_raw), "./rec_%s_%ds.es",
        (g_codec==VE_TYPE_H265)?"h265":"h264", g_secs);

    if (g_dump_raw) {
        g_raw = fopen(path_raw,"wb");
        if (!g_raw) perror("fopen raw");
    }

    if (!g_no_mp4) {
        // ★ 这里第二个参数务必传 1（单路），避免库当作“通道范围”
        g_mp4 = HiF_media_mp4_open_file(path_mp4, 1);
        if (!g_mp4) {
            fprintf(stderr,"open mp4 failed\n");
            if (g_raw){ fclose(g_raw); g_raw=NULL; }
            HiF_media_exit();
            return 3;
        }
    }

    if (HiF_media_start_encode() != 0) {
        fprintf(stderr, "start_encode failed\n");
        if (g_mp4){ HiF_media_mp4_close_file(g_mp4); g_mp4=NULL; }
        if (g_raw){ fclose(g_raw); g_raw=NULL; }
        HiF_media_exit(); return 4;
    }

    g_t0_ms = now_ms();
    while (!g_stop) { usleep(50*1000); }

    // ★ 正确的收尾顺序：先停编码/退出媒体 → 再关文件句柄
    g_closing = 1;                   // 告诉回调别再写
    usleep(100*1000);                // 给回调一点时间退出
   
    pthread_mutex_lock(&g_io_mtx);
    if (g_mp4){ HiF_media_mp4_close_file(g_mp4); g_mp4=NULL; }
    if (g_raw){ fflush(g_raw); fclose(g_raw); g_raw=NULL; }
    pthread_mutex_unlock(&g_io_mtx);
    HiF_media_exit();                // 停止线程与回调

    if (!g_no_mp4) printf("saved: %s\n", path_mp4);
    if (g_dump_raw) printf("saved: %s\n", path_raw);
    return 0;
}
