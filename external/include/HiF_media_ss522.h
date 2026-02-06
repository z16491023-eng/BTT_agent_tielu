#ifndef _SS522_H_
#define _SS522_H_

#define CHN_NUM_MAX    4

/*
    支持的摄像机类型
    可以通过 HiF_media_get_cam_mode() 获取当前摄像机类型
*/
#define VIDEO_MODE_1080P25  3     //加载tp2830驱动的时候需要指定 mode=3， insmod hi_tp2830.ko mode=3
#define VIDEO_MODE_PAL      8
#define VIDEO_MODE_NTSC     9

/*
    请根据摄像机类型配置以下分辨率
*/
typedef enum {
    HIF_PIC_CIF,
    HIF_PIC_360P,    /* 640 * 360 */
    HIF_PIC_D1_PAL,  /* 720 * 576 */
    HIF_PIC_D1_NTSC, /* 720 * 480 */
    HIF_PIC_960H,      /* 960 * 576 */
    HIF_PIC_720P,    /* 1280 * 720 */
    HIF_PIC_1080P,   /* 1920 * 1080 */
    HIF_PIC_BUTT
} hif_pic_size;

typedef enum _frame_type_
{
	FRAME_I = 0,
	FRAME_P = 1, 
	FRAME_B = 2,
	FRAME_ERR,
} FRAMETYPE;

typedef enum _video_encode_format_
{
    VE_TYPE_H264 = 96,
    VE_TYPE_H265 = 265,
} hif_payload_type;;

typedef struct _frame_info_
{
    int chn;
	hif_payload_type encode_type;
	unsigned short height;
	unsigned short width;
	unsigned char frame_rate;
	unsigned int frame_seq;
	FRAMETYPE frame_type;
	unsigned long timestamp;  // ms
} FRAME_INFO;

typedef enum _venc_rc_mode_
{
	VENC_RC_CBR   = 0,
    VENC_RC_VBR   = 1,
    VENC_RC_AVBR  = 2,
    VENC_RC_CVBR  = 3,
    VENC_RC_QVBR  = 4,
    VENC_RC_ERR,
}hif_rc_mode;


typedef enum _venc_rc_qpmode_
{
    VENC_RC_QPMAP_MODE_MEAN_QP = 0,
    VENC_RC_QPMAP_MODE_MIN_QP  = 1,
    VENC_RC_QPMAP_MODE_MAX_QP  = 2,
    VENC_RC_QPMAP_MODE_ERR,
}VENCRCQPMODE;


typedef enum {
    VENC_GOP_MODE_NORMAL_P    = 0,
    VENC_GOP_MODE_DUAL_P      = 1,
    VENC_GOP_MODE_SMART_P     = 2,
    VENC_GOP_MODE_ADV_SMART_P = 3,
    VENC_GOP_MODE_BIPRED_B    = 4,
    VENC_GOP_MODE_LOW_DELAY_B = 5,
    VENC_GOP_MODE_BUTT,
} hif_venc_gop_mode;

typedef struct
{
    unsigned char ch;
    unsigned char is_lost;  //0: 正常 1: 丢失
} hif_video_loss_t;

typedef struct
{
    unsigned char ch;
    unsigned char mode;
    unsigned char std;
} hif_video_mode_t;

typedef struct 
{	
      int iUnused; 
}* hif_mp4_handle;


//码流回调函数定义
typedef int (*HiF_media_cb_get_stream)(FRAME_INFO frame_info, char* stream_buf, int buf_len);


typedef struct _hif_media_param_t
{
    hif_pic_size pic_size;
    hif_payload_type payload;       
    hif_venc_gop_mode gop_mode;
    hif_rc_mode rc_mode;
    unsigned int bit_rate;
    unsigned int frame_rate;
    unsigned int max_bit_rate;
    unsigned int long_term_min_bit_rate;
    HiF_media_cb_get_stream cb_get_stream;
}hif_media_param_t;


#if _cplusplus
extern "C" {
#endif

/*
说明： 如果没有特殊说明，所有函数的返回值都是
        成功返回0
        失败返回负数
*/

/*
函数功能:    编码模块初始化
*/
int HiF_media_init();

/*
函数功能:    停止编码，释放全部资源
*/
int HiF_media_exit();

/*
函数功能:    多媒体参数获取/设置
*/
int HiF_media_get_param(int ch, hif_media_param_t* param);
int HiF_media_set_param(int ch, hif_media_param_t* param);

/*
函数功能:    开始编码, 必须在设置参数(HiF_media_set_param)之后调用
*/
int HiF_media_start_encode();

/*
@brief: 获取一路摄像机状态,
@param： hif_video_loss_t 中的ch为要查询的通道号， is_lost为0表示正常，1表示丢失
*/
int HiF_media_get_cam_status( hif_video_loss_t *st_cam);


/*
@brief: 获取一路摄像机制式,
@param： hif_video_mode_t 中的ch为要查询的通道号, mode 为视频制式； std 为摄像机类型
        mode_str: 返回视频制式字符串, 传入的字符串缓冲区至少16字节

#define VIDEO_MODE_NTSC     9
#define VIDEO_MODE_PAL      8
#define VIDEO_MODE_1080P25  3
*/
int HiF_media_get_cam_mode( hif_video_mode_t *st_cam, char* mode_str);

/*
@函数功能:    抓拍一张图片
@param: ch:       抓拍通道号
@param: jpg_buf:  用于存储抓拍的JPEG图片数据的缓冲区
@param: buf_len:  缓冲区的长度
@返回值:      成功返回数据长度
            失败返回-1
*/
int HiF_media_snap(unsigned char ch, unsigned char *jpg_buf, int buf_len);


/*------------------------------mp4文件操作--------------------------*/

hif_mp4_handle HiF_media_mp4_open_file(char* file_name, int n_chn);

//返回写入的字节， 返回 0 当前帧没有写入， 可能是因为第一帧不是 I 帧
int HiF_media_mp4_write_frame(hif_mp4_handle fp_mp4, unsigned char* data, int len, int pts);

int HiF_media_mp4_close_file(hif_mp4_handle fp_mp4);

/*-------------------------系统信息获取-------------------*/
/*
@brief: 
@param usage_percent: 内存使用率，百分比
@param total_kb_out: 总内存，单位KB
@param avail_kb_out: 可用内存，单位KB
*/
int HiF_sys_get_mem_usage(double *usage_percent, unsigned long *total_kb_out, unsigned long *avail_kb_out);
/*
@brief:
@param: usage_percent: CPU使用率，百分比
@param: sample_ms: 采样时间，单位毫秒
*/
int HiF_sys_get_cpu_usage(double *usage_percent, unsigned int sample_ms);
/*
@brief:
@param: usage_percent: 可写分区使用率，百分比
@param: total_kb: 总空间，单位KB
@param: used_kb: 已用空间，单位KB
@param: avail_kb: 可用空间，单位KB
*/
int HiF_sys_get_fs_usage(double *usage_percent,  unsigned long *total_kb,unsigned long *used_kb, unsigned long *avail_kb);

#if _cplusplus
}
#endif

#endif
