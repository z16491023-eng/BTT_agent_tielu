#pragma once
#include "hgs_type.h"

#define YX_IF_NAME  "eth0"

typedef struct _hgs_net_param
{
    // char if_name[32];          //网卡名称
    char ipv4[32];            //ip地址
    char netmask[32];          //子网掩码
    char gateway[32];       //网关地址
    char mac[32];           //物理地址
}hgs_net_config_t;

typedef enum {
    SERIAL_TYPE_RS232 = 0,  //座子上的 RS232
    SERIAL_TYPE_RS485,      //座子上的 485
    SERIAL_TYPE_UART          //蓝牙通信的 UART
} hgs_serial_type_e;

typedef struct {
    hgs_serial_type_e type;       // RS232 / RS485
    int baudrate;             // 波特率
    int databits;             // 数据位 (5,6,7,8)
    int stopbits;             // 停止位 (1,2)
    char parity;              // 奇偶校验 'N','E','O'
} hgs_serial_config_t;


//初始化， 必须在misc所有函数之前调用
void hgs_misc_init();

int hgs_set_net_param(hgs_net_config_t* param);

int hgs_set_dns( const char* dns1, const char* dns2);

/*
摄像机电源控制
chn:  通道号， 0-3
on:   1: 打开， 0：关闭
返回： 0 成功， -1 失败
*/
int hgs_cam_power_control(unsigned char chn, unsigned char on);

//----------------------serial port----------------------
/*
根据hgs_serial_type_e 打开指定的串口
返回： >0 成功， 串口句柄
        -1 失败
*/
int hgs_serial_open(hgs_serial_config_t* cfg);

/*
关闭串口
fd: open 返回的句柄
返回值： 0 成功， -1 失败
*/
int hgs_serial_close(int fd);

/*
返回：发送的字节数
*/
int hgs_serial_write(int fd, const uint8_t *data, int len);

/*
fd： open 返回的句柄
buf： 用于存放读取的数据
len： 期望读取的字节数
返回值： 实际读取的字节数， 0表示没有数据， -1表示失败
*/
int hgs_serial_read(int fd, uint8_t *buf, int len);