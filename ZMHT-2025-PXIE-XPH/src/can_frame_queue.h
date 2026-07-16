#ifndef CAN_FRAME_QUEUE_H
#define CAN_FRAME_QUEUE_H

#include <pthread.h>
#include <time.h>
// 包含驱动头文件，使用其定义的axican_frame
#include "axican/zmuav_axican.h"

// CAN通道数量（根据实际硬件调整）
 #define AXICAN_CHAN_MAX 6

// 队列节点结构（存储一帧数据）
typedef struct CanFrameNode {
    struct axican_frame frame;  // 使用驱动定义的axican_frame
    int can_id;                 // 所属CAN通道ID
    struct timespec timestamp;  // 接收时间戳（系统时间）
    struct CanFrameNode* next;  // 队列链表指针
    struct CanFrameNode* pool_next;  // 内存池链表指针
} CanFrameNode;

// 队列结构（管理多个节点）
typedef struct CanFrameQueue {
    CanFrameNode* head;        // 队列头部
    CanFrameNode* tail;        // 队列尾部
    CanFrameNode* pool_head;   // 内存池头部（空闲节点）
    pthread_mutex_t mutex;     // 互斥锁
    pthread_cond_t cond;       // 条件变量
    int count;                 // 当前队列中的帧数
    int max_size;              // 队列最大容量
} CanFrameQueue;

// 批量入队临时缓存结构
typedef struct BatchFrame {
    struct axican_frame frame;  // 使用驱动定义的axican_frame
    struct timespec ts;         // 时间戳
    int can_id;                 // 通道ID
} BatchFrame;

// 全局队列数组
extern CanFrameQueue can_queues[AXICAN_CHAN_MAX];
extern CanFrameQueue can_queues_1[AXICAN_CHAN_MAX];

// 队列操作函数声明（非静态）
void can_queue_init(CanFrameQueue* queue, int max_size);
int can_queue_enqueue_batch(CanFrameQueue* queue, BatchFrame* batch, int batch_size);
int can_queue_dequeue_batch(CanFrameQueue* queue, CanFrameNode** nodes, int max_count);
void can_queue_recycle_nodes(CanFrameQueue* queue, CanFrameNode** nodes, int node_count);
void can_queue_destroy(CanFrameQueue* queue);

#endif  // CAN_FRAME_QUEUE_H
