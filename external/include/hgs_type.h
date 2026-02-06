
#ifndef   __ESL_TYPE_H__
#define   __ESL_TYPE_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <errno.h>

#include <sys/types.h>
#include <netdb.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
    
#define ESL_OK              0
#define ESL_FAIL            -1

typedef unsigned char       u8;
typedef unsigned short      u16;
typedef unsigned int        u32;

#define STAND_STR_LEN   128


#define _LD_DEBUG_

#ifdef _LD_DEBUG_
#define iv_dpf(fmt,args...)    do {printf("[HGS] [%s %d]: " fmt, __FUNCTION__, __LINE__, ## args); printf("\n");}while(0)
#else
#define iv_dpf(fmt,args...)
#endif

#ifdef _LD_DEBUG_
#define dpf(fmt,args...)    do {printf("[HGS] [%s %d]: " fmt, __FUNCTION__, __LINE__, ## args); printf("\n");}while(0)
#else
#define dpf(fmt,args...)
#endif

#define iv_epf(fmt,args...)    do {printf("\033[;35;1mHGS] [%s %d]: " fmt, __FUNCTION__, __LINE__, ## args); printf("\033[0m\n");}while(0)
#define epf(fmt,args...)    do {printf("\033[;35;1mHGS] [%s %d]: " fmt, __FUNCTION__, __LINE__, ## args); printf("\033[0m\n");}while(0)


#endif