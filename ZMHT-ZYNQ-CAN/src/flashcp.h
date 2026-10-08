#ifndef _FLASHCP_H_
#define _FLASHCP_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif 

/**
 * @brief Flash烧写设备信息结构体
 */
typedef struct {
    char* devicename;          // MTD设备名（如/dev/mtd0）
    uint32_t size;             // 待写入数据长度
    unsigned char *buff;       // 待写入数据缓冲区
} device_info_t;

/**
 * @brief 核心烧写函数：擦除Flash→写入数据→校验数据
 * @param info 设备/数据信息
 * @return 0成功，-1打开失败，-2非MTD设备，-3擦除失败，-4写入失败，-5校验失败
 */
int flashcp_main(device_info_t* info);

#ifdef __cplusplus
}
#endif

#endif  /* _FLASHCP_H_ */