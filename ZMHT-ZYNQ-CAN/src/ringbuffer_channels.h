#ifndef _RINGBUFFER_H_
#define _RINGBUFFER_H_

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

// -------------------------- 3种数据类型定义 --------------------------
/**
 * @brief 模拟器数据结构（PC与ARM通信）
 */

// -------------------------- 环形缓冲区基础结构 --------------------------
typedef struct {
    uint8_t *data;              // 实际数据存储缓冲区
    uint32_t in;                // 写入指针（元素计数）
    uint32_t out;               // 读取指针（元素计数）
    uint32_t mask;              // 索引掩码（容量-1，用于快速取模）
    uint32_t esize;             // 单个元素大小（字节）
} ring_buffer_t;

// -------------------------- 32通道缓冲区管理结构 --------------------------
/**
 * @brief 32通道缓冲区集合（每种数据类型一个集合）
 */
// typedef struct {
//     ring_buffer_t *channels[32];// 32个通道的缓冲区
//     uint32_t elem_size;         // 该集合中单个元素的大小（字节）
//     uint32_t buf_capacity;      // 每个通道的缓冲区容量（元素个数）
// } RingBuffer32ChSet;
typedef struct {
    ring_buffer_t **channels;// 32个通道的缓冲区
    uint8_t max_channel;
    uint32_t elem_size;         // 该集合中单个元素的大小（字节）
    uint32_t buf_capacity;      // 每个通道的缓冲区容量（元素个数）
} RingBuffer32ChSet;
extern RingBuffer32ChSet g_simulator_buf_set;   // 模拟器数据32通道集合
extern RingBuffer32ChSet g_data_buf_set;        // 核心数据32通道集合
extern RingBuffer32ChSet g_pulse_buf_set;       // 秒脉冲数据32通道集合
extern RingBuffer32ChSet g_can_buf_set;         // CAN数据多通道集合（示例：6通道）
extern RingBuffer32ChSet g_poll_buf_set;


// -------------------------- 基础缓冲区接口 --------------------------
#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief 创建单个环形缓冲区
 * @param size 缓冲区总大小（字节）
 * @param esize 单个元素大小（字节）
 * @return 成功返回缓冲区指针，失败返回NULL
 */
ring_buffer_t* ringbuffer_create(uint32_t size, uint32_t esize);

/**
 * @brief 销毁单个环形缓冲区
 * @param rb 待销毁的缓冲区指针
 */
void ringbuffer_destory(ring_buffer_t* rb);

/**
 * @brief 判断缓冲区是否已满
 * @param rb 缓冲区指针
 * @return true=已满，false=未满
 */
bool ringbuffer_is_full(ring_buffer_t *rb);

/**
 * @brief 判断缓冲区是否为空
 * @param rb 缓冲区指针
 * @return true=为空，false=非空
 */
bool ringbuffer_is_empty(ring_buffer_t *rb);

/**
 * @brief 向缓冲区写入数据（单个元素）
 * @param rb 缓冲区指针
 * @param buf 待写入数据的指针
 * @param len 数据长度（需等于元素大小，仅作校验）
 * @return 成功写入的元素个数（1=成功，0=失败）
 */
uint32_t ringbuffer_put(ring_buffer_t *rb, const void *buf, uint32_t len);

/**
 * @brief 从缓冲区读取数据（单个元素）
 * @param rb 缓冲区指针
 * @param buf 存储读取数据的指针
 * @param len 数据长度（需等于元素大小，仅作校验）
 * @return 成功读取的元素个数（1=成功，0=失败）
 */
uint32_t ringbuffer_get(ring_buffer_t *rb, void *buf, uint32_t len);

/**
 * @brief 获取缓冲区中已存储的元素个数
 * @param rb 缓冲区指针
 * @return 已存储元素个数
 */
uint32_t ringbuffer_len(ring_buffer_t *rb);

/**
 * @brief 获取缓冲区的总容量（元素个数）
 * @param rb 缓冲区指针
 * @return 总容量（元素个数）
 */
uint32_t ringbuffer_cap(ring_buffer_t *rb);

/**
 * @brief 获取缓冲区的可用容量（可写入元素个数）
 * @param rb 缓冲区指针
 * @return 可用容量（元素个数）
 */
uint32_t ringbuffer_avail(ring_buffer_t *rb);

/**
 * @brief 重置缓冲区（清空所有数据，指针归位）
 * @param rb 缓冲区指针
 */
void ringbuffer_reset(ring_buffer_t *rb);

// -------------------------- 32通道缓冲区接口 --------------------------
/**
 * @brief 初始化32通道缓冲区集合
 * @param set 缓冲区集合指针
 * @param elem_size 单个元素大小（字节）
 * @param buf_capacity 每个通道的缓冲区容量（元素个数，自动调整为2的幂）
 * @return 0=成功，-1=失败
 */
int ringbuffer_32ch_init(RingBuffer32ChSet *set, uint32_t elem_size, uint32_t buf_capacity,uint8_t MAX_Channel);

/**
 * @brief 销毁32通道缓冲区集合
 * @param set 缓冲区集合指针
 */
void ringbuffer_32ch_destory(RingBuffer32ChSet *set,uint8_t MAX_Channel);

/**
 * @brief 向32通道集合的指定通道写入数据
 * @param set 缓冲区集合指针
 * @param ch 通道号（0-31）
 * @param data 待写入数据的指针
 * @return 1=成功，0=失败（通道无效或缓冲区满）
 */
uint32_t ringbuffer_32ch_put(RingBuffer32ChSet *set, uint8_t ch, const void *data,uint8_t MAX_Channel);

/**
 * @brief 从32通道集合的指定通道读取数据
 * @param set 缓冲区集合指针
 * @param ch 通道号（0-31）
 * @param data 存储读取数据的指针
 * @return 1=成功，0=失败（通道无效或缓冲区空）
 */
uint32_t ringbuffer_32ch_get(RingBuffer32ChSet *set, uint8_t ch, void *data,uint8_t MAX_Channel);

/**
 * @brief 获取32通道集合指定通道的已存储元素个数
 * @param set 缓冲区集合指针
 * @param ch 通道号（0-31）
 * @return 已存储元素个数（通道无效返回0）
 */
uint32_t ringbuffer_32ch_len(RingBuffer32ChSet *set, uint8_t ch,uint8_t MAX_Channel);

/**
 * @brief 重置32通道集合指定通道的缓冲区
 * @param set 缓冲区集合指针
 * @param ch 通道号（0-31）
 */
void ringbuffer_32ch_reset(RingBuffer32ChSet *set, uint8_t ch,uint8_t MAX_Channel);

uint32_t ringbuffer_32ch_avail(RingBuffer32ChSet *set, uint8_t ch,uint8_t MAX_Channel);

#ifdef __cplusplus
}
#endif

#endif /* _RINGBUFFER_H_ */