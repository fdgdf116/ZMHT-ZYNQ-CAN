#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <assert.h>

#include "ringbuffer_channels.h"
RingBuffer32ChSet g_simulator_buf_set;
RingBuffer32ChSet g_data_buf_set;
RingBuffer32ChSet g_pulse_buf_set;
// -------------------------- 基础工具宏与函数 --------------------------
#define rb_min(X, Y)				\
	({ typeof(X) __x = (X);			\
		typeof(Y) __y = (Y);		\
		(__x < __y) ? __x : __y; })

/**
 * @brief 判断数值是否为2的幂
 */
static inline int is_power_of_2(uint32_t n)
{
	return (n != 0 && ((n & (n - 1)) == 0));
}

/**
 * @brief 将数值向下取整为最近的2的幂
 */
static inline uint32_t rounddown_pow_of_two(uint32_t n)
{
	if (n == 0)
		return 0;
	n |= n >> 1;
	n |= n >> 2;
	n |= n >> 4;
	n |= n >> 8;
	n |= n >> 16;
	return (n + 1) >> 1;
}

// -------------------------- 单个环形缓冲区实现 --------------------------
ring_buffer_t* ringbuffer_create(uint32_t size, uint32_t esize)
{
	ring_buffer_t *rb = NULL;
    uint8_t *buf = NULL;
	uint32_t elem_count = 0;  // 元素个数（总大小/单个元素大小）
	uint32_t buf_total_size = 0;  // 缓冲区总字节数

	// 1. 校验参数有效性
	if (esize == 0 || size == 0) {
		errno = EINVAL;
		return NULL;
	}

	// 2. 分配缓冲区控制结构体
	rb = (ring_buffer_t *)malloc(sizeof(*rb));
    if (rb == NULL) {
        goto exit;
    }
	memset(rb, 0, sizeof(*rb));

	// 3. 计算元素个数（自动调整为2的幂）
	elem_count = size / esize;  // 总字节数 → 元素个数
	if (!is_power_of_2(elem_count)) {
		elem_count = rounddown_pow_of_two(elem_count);
	}
	// 若调整后元素个数为0，强制设为1（最小容量）
	if (elem_count == 0) {
		elem_count = 1;
	}

	// 4. 分配实际数据缓冲区
	buf_total_size = elem_count * esize;
	buf = (uint8_t *)malloc(buf_total_size);
	if (buf == NULL) {
		free(rb);
		rb = NULL;
		goto exit;
	}
	memset(buf, 0, buf_total_size);

	// 5. 初始化缓冲区控制参数
	rb->in = 0;                  // 写入指针初始化为0
	rb->out = 0;                 // 读取指针初始化为0
	rb->esize = esize;           // 单个元素大小
	rb->data = buf;              // 数据缓冲区指针
	rb->mask = elem_count - 1;   // 索引掩码（替代取模运算，提升效率）

exit:
	return rb;
}

void ringbuffer_destory(ring_buffer_t* rb)
{
    if (rb != NULL) {
        if (rb->data != NULL) {
            free(rb->data);
            rb->data = NULL;
        }
        free(rb);
        rb = NULL;
    }
    return ;
}

/**
 * @brief 内部函数：将数据拷贝到缓冲区（处理环形绕回）
 */
static void ringbuffer_copy_in(ring_buffer_t *rb, const void *src, uint32_t len, uint32_t off)
{
	uint32_t elem_total = rb->mask + 1;  // 总元素个数
	uint32_t esize = rb->esize;          // 单个元素大小
	uint32_t copy_bytes = len * esize;   // 需拷贝的总字节数
	uint32_t off_bytes = 0;              // 偏移字节数
	uint32_t first_copy = 0;             // 第一段拷贝字节数

	// 1. 计算实际字节偏移（支持元素级偏移）
	off &= rb->mask;  // 确保偏移在有效范围内
	off_bytes = off * esize;

	// 2. 计算第一段拷贝长度（避免跨缓冲区边界）
	first_copy = rb_min(copy_bytes, (elem_total * esize) - off_bytes);

	// 3. 分两段拷贝（处理绕回）
	memcpy(rb->data + off_bytes, src, first_copy);
	memcpy(rb->data, (const uint8_t*)src + first_copy, copy_bytes - first_copy);

	// 4. 内存屏障：确保数据写入完成后再更新指针（多线程安全）
	__sync_synchronize();
}

/**
 * @brief 内部函数：计算缓冲区未使用的元素个数
 */
static inline unsigned int ringbuffer_unused(ring_buffer_t *rb)
{
	return (rb->mask + 1) - (rb->in - rb->out);
}

uint32_t ringbuffer_put(ring_buffer_t *rb, const void *buf, uint32_t len)
{
    uint32_t unused_elem = 0;
	uint32_t put_elem = 0;  // 实际写入的元素个数

	// 1. 校验参数
	if (rb == NULL || buf == NULL || len == 0) {
		errno = EINVAL;
		return 0;
	}

	// 2. 仅支持单次写入1个元素（确保数据完整性）
	put_elem = (len * rb->esize >= rb->esize) ? 1 : 0;
	if (put_elem == 0) {
		errno = EINVAL;
		return 0;
	}

	// 3. 检查缓冲区剩余空间
	unused_elem = ringbuffer_unused(rb);
	if (put_elem > unused_elem) {
		return 0;  // 空间不足，写入失败
	}

	// 4. 拷贝数据并更新写入指针
	ringbuffer_copy_in(rb, buf, put_elem, rb->in);
	rb->in += put_elem;

	return put_elem;
}

/**
 * @brief 内部函数：从缓冲区拷贝数据（处理环形绕回）
 */
static void ringbuffer_copy_out(ring_buffer_t *rb, void *dst, uint32_t len, uint32_t off)
{
	uint32_t elem_total = rb->mask + 1;  // 总元素个数
	uint32_t esize = rb->esize;          // 单个元素大小
	uint32_t copy_bytes = len * esize;   // 需拷贝的总字节数
	uint32_t off_bytes = 0;              // 偏移字节数
	uint32_t first_copy = 0;             // 第一段拷贝字节数

	// 1. 计算实际字节偏移（支持元素级偏移）
	off &= rb->mask;  // 确保偏移在有效范围内
	off_bytes = off * esize;

	// 2. 计算第一段拷贝长度（避免跨缓冲区边界）
	first_copy = rb_min(copy_bytes, (elem_total * esize) - off_bytes);

	// 3. 分两段拷贝（处理绕回）
	memcpy(dst, rb->data + off_bytes, first_copy);
	memcpy((uint8_t*)dst + first_copy, rb->data, copy_bytes - first_copy);

	// 4. 内存屏障：确保数据读取完成后再更新指针（多线程安全）
	__sync_synchronize();
}

/**
 * @brief 内部函数：预览缓冲区数据（不移动读取指针）
 */
static uint32_t ringbuffer_out_peek(ring_buffer_t *rb, void *buf, uint32_t len)
{
	uint32_t used_elem = 0;  // 已使用元素个数
	uint32_t peek_elem = 0;  // 预览元素个数

	if (rb == NULL || buf == NULL || len == 0) {
		return 0;
	}

	// 1. 计算可预览的元素个数
	used_elem = rb->in - rb->out;
	peek_elem = rb_min(len, used_elem);
	if (peek_elem == 0) {
		return 0;
	}

	// 2. 拷贝数据（不更新指针）
	ringbuffer_copy_out(rb, buf, peek_elem, rb->out);
	return peek_elem;
}

uint32_t ringbuffer_get(ring_buffer_t *rb, void *buf, uint32_t len)
{
	uint32_t get_elem = 0;  // 实际读取的元素个数

	// 1. 校验参数
	if (rb == NULL || buf == NULL || len == 0) {
		errno = EINVAL;
		return 0;
	}

	// 2. 仅支持单次读取1个元素
	get_elem = (len * rb->esize >= rb->esize) ? 1 : 0;
	if (get_elem == 0) {
		errno = EINVAL;
		return 0;
	}

	// 3. 读取数据并更新读取指针
	get_elem = ringbuffer_out_peek(rb, buf, get_elem);
	if (get_elem > 0) {
		rb->out += get_elem;
	}

	return get_elem;
}

uint32_t ringbuffer_len(ring_buffer_t *rb)
{
    if (rb == NULL) {
        return 0;
    }
    return rb->in - rb->out;
}

uint32_t ringbuffer_cap(ring_buffer_t *rb)
{
    if (rb == NULL) {
        return 0;
    }
    return rb->mask + 1;
}

uint32_t ringbuffer_avail(ring_buffer_t *rb)
{
    if (rb == NULL) {
        return 0;
    }
    return ringbuffer_cap(rb) - ringbuffer_len(rb);
}

bool ringbuffer_is_full(ring_buffer_t *rb)
{
    if (rb == NULL) {
        return false;
    }
    return ringbuffer_len(rb) >= (rb->mask + 1);
}

bool ringbuffer_is_empty(ring_buffer_t *rb)
{
    if (rb == NULL) {
        return true;
    }
    return rb->in == rb->out;
}

void ringbuffer_reset(ring_buffer_t *rb)
{
	if (rb != NULL) {
		// 多线程环境下应考虑加锁保护
		rb->in = 0;
		rb->out = 0;
	}
}

// -------------------------- 32通道缓冲区实现 --------------------------

/**
 * @brief 初始化32通道缓冲区集合
 */
int ringbuffer_32ch_init(RingBuffer32ChSet *set, uint32_t elem_size, uint32_t buf_capacity,uint8_t MAX_Channel)
{
  int i;
    uint32_t buf_size;         // 每个通道的总字节数
    uint32_t actual_cap = 0;   // 调整为2的幂后的实际容量

    // 1. 参数合法性校验
    if (set == NULL || elem_size == 0 || buf_capacity == 0 || MAX_Channel == 0) {
        errno = EINVAL;
        perror("ringbuffer_32ch_init: invalid parameters");
        return -1;
    }

    // 2. 初始化集合元数据
    memset(set, 0, sizeof(RingBuffer32ChSet));
    set->elem_size = elem_size;
    set->max_channel = MAX_Channel;  // 记录自定义通道总数

    // 3. 调整容量为2的幂（提升取模效率）
    actual_cap = buf_capacity;
    if (!is_power_of_2(actual_cap)) {
        actual_cap = rounddown_pow_of_two(actual_cap);
    }
    if (actual_cap == 0) {
        actual_cap = 1;  // 最小容量保障
    }
    set->buf_capacity = actual_cap;

    // 4. 动态分配通道数组（核心：支持任意通道数）
    set->channels = (ring_buffer_t **)malloc(sizeof(ring_buffer_t *) * MAX_Channel);
    if (set->channels == NULL) {
        perror("ringbuffer_32ch_init: malloc channels failed");
        return -1;
    }
    memset(set->channels, 0, sizeof(ring_buffer_t *) * MAX_Channel);  // 避免野指针

    // 5. 为每个通道创建缓冲区
    buf_size = actual_cap * elem_size;  // 每个通道的总字节数
    for (i = 0; i < MAX_Channel; i++) {
        set->channels[i] = ringbuffer_create(buf_size, elem_size);
        if (set->channels[i] == NULL) {
            perror("ringbuffer_32ch_init: create channel buffer failed");
            // 回滚：释放已创建的通道资源
            for (int j = 0; j < i; j++) {
                ringbuffer_destory(set->channels[j]);
                set->channels[j] = NULL;
            }
            free(set->channels);
            set->channels = NULL;
            return -1;
        }
    }

    return 0;
}

/**
 * @brief 销毁32通道缓冲区集合
 */
void ringbuffer_32ch_destory(RingBuffer32ChSet *set,uint8_t MAX_Channel)
{
    int i;
    if (set == NULL) {
        return;
    }

    // 销毁每个通道的缓冲区
    for (i = 0; i <  MAX_Channel; i++) {
        ringbuffer_destory(set->channels[i]);
        set->channels[i] = NULL;
    }

    // 清除集合元数据
    memset(set, 0, sizeof(RingBuffer32ChSet));
}

/**
 * @brief 向32通道集合的指定通道写入数据
 */
uint32_t ringbuffer_32ch_put(RingBuffer32ChSet *set, uint8_t ch, const void *data,uint8_t MAX_Channel)
{
    // 1. 校验参数
    if (set == NULL || data == NULL || ch >= MAX_Channel || set->channels[ch] == NULL) {
        errno = EINVAL;
        return 0;
    }

    // 2. 向指定通道写入数据
    return ringbuffer_put(set->channels[ch], data, 1);
}

/**
 * @brief 从32通道集合的指定通道读取数据
 */
uint32_t ringbuffer_32ch_get(RingBuffer32ChSet *set, uint8_t ch, void *data,uint8_t MAX_Channel)
{
    // 1. 校验参数
    if (set == NULL || data == NULL || ch >= MAX_Channel || set->channels[ch] == NULL) {
        errno = EINVAL;
        return 0;
    }

    // 2. 从指定通道读取数据
    return ringbuffer_get(set->channels[ch], data, 1);
}

/**
 * @brief 获取32通道集合指定通道的已存储元素个数
 */
uint32_t ringbuffer_32ch_len(RingBuffer32ChSet *set, uint8_t ch,uint8_t MAX_Channel)
{
    if (set == NULL || ch >= MAX_Channel || set->channels[ch] == NULL) {
        return 0;
    }
    return ringbuffer_len(set->channels[ch]);
}

/**
 * @brief 重置32通道集合指定通道的缓冲区
 */
void ringbuffer_32ch_reset(RingBuffer32ChSet *set, uint8_t ch,uint8_t MAX_Channel)
{
    if (set != NULL && ch < MAX_Channel && set->channels[ch] != NULL) {
        ringbuffer_reset(set->channels[ch]);
    }
}
uint32_t ringbuffer_32ch_avail(RingBuffer32ChSet *set, uint8_t ch,uint8_t MAX_Channel) {
    // 校验参数合法性
    if (set == NULL || ch >= MAX_Channel|| set->channels[ch] == NULL) {
        return 0; // 无效参数，返回 0 表示无可用空间
    }
    // 调用单通道缓冲区的可用空间函数
    return ringbuffer_avail(set->channels[ch]);
}


