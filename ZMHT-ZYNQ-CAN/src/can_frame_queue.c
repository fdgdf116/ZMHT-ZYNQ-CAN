#include "can_frame_queue.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// 全局队列实例化
CanFrameQueue can_queues[AXICAN_CHAN_MAX];
CanFrameQueue can_queue_emergency[AXICAN_CHAN_MAX];

// 初始化队列及内存池（移除static）
void can_queue_init(CanFrameQueue* queue, int max_size) {
    if (!queue || max_size <= 0) {
        printf("[can_queue_init] 无效参数\n");
        return;
    }

    // 初始化队列基本字段
    queue->head = NULL;
    queue->tail = NULL;
    queue->count = 0;
    queue->max_size = max_size;

    // 预分配内存池
    CanFrameNode* pool = (CanFrameNode*)malloc(max_size * sizeof(CanFrameNode));
    if (!pool) {
        printf("[can_queue_init] 内存池分配失败（大小：%d)\n", max_size);
        return;
    }

    // 初始化内存池链表
    queue->pool_head = pool;
    for (int i = 0; i < max_size - 1; i++) {
        pool[i].pool_next = &pool[i + 1];
    }
    pool[max_size - 1].pool_next = NULL;

    // 初始化锁和条件变量
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond, NULL);
}

// 批量入队实现（移除static）
int can_queue_enqueue_batch(CanFrameQueue* queue, BatchFrame* batch, int batch_size) {
    if (!queue || !batch || batch_size <= 0) {
        return -1;
    }


    pthread_mutex_lock(&queue->mutex);
    queue->count += batch_size;

    // 等待队列有足够空间
    while (queue->count + batch_size > queue->max_size) {
        static int warn_cnt = 0;
        if (warn_cnt % 100 == 0) {
            printf("[CAN%d] 队列满（当前:%d, 需入队:%d)\n", 
                   batch[0].can_id, queue->count, batch_size);
        }
        warn_cnt++;
        pthread_cond_wait(&queue->cond, &queue->mutex);
    }

    // 批量添加节点
    CanFrameNode* last_node = NULL;
    for (int i = 0; i < batch_size; i++) {
        // 从内存池取节点
        CanFrameNode* node = queue->pool_head;
        if (!node) {
            pthread_mutex_unlock(&queue->mutex);
            return -1;
        }
        queue->pool_head = node->pool_next;

        // 复制数据
        node->frame = batch[i].frame;
        node->can_id = batch[i].can_id;
        node->timestamp = batch[i].ts;
        node->next = NULL;

        // 串联节点
        if (last_node) {
            last_node->next = node;
        } else {
            if (!queue->tail) {
                queue->head = node;
            } else {
                queue->tail->next = node;
            }
        }
        last_node = node;
    }

    // 更新队列状态
    if (last_node) {
        queue->tail = last_node;
    }


    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);

    return 0;
}

// 批量出队实现（移除static）
int can_queue_dequeue_batch(CanFrameQueue* queue, CanFrameNode** nodes, int max_count) {
    // 参数有效性检查
    if (!queue || !nodes || max_count <= 0) {
        return -1;
    }

    pthread_mutex_lock(&queue->mutex);

    // 等待队列有数据
    while (queue->count == 0) {
        // 检查等待是否被信号中断
        int ret = pthread_cond_wait(&queue->cond, &queue->mutex);
        if (ret != 0) {
            pthread_mutex_unlock(&queue->mutex);
            return -1; // 等待出错
        }
    }

    // 计算实际可出队数量，确保不超过队列实际数量
    int take_count = (queue->count < max_count) ? queue->count : max_count;
    CanFrameNode* current = queue->head;

    //收集出队节点，增加指针有效性检查
    for (int i = 0; i < take_count; i++) {
        if (!current) { // 意外的NULL指针，防止崩溃
            take_count = i; // 修正实际取出的数量
            break;
        }
        nodes[i] = current;
        current = current->next;
    }

    // 更新队列状态
    queue->head = current;
    if (!queue->head) {
        queue->tail = NULL; // 队列为空时尾指针也置空
    }
    queue->count -= take_count;

    // 通知可能等待的入队线程
    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);

    return take_count;
}

// 回收节点到内存池（移除static）
void can_queue_recycle_nodes(CanFrameQueue* queue, CanFrameNode** nodes, int node_count) {
    if (!queue || !nodes || node_count <= 0) {
        return;
    }

    pthread_mutex_lock(&queue->mutex);

    // 将回收节点串联到内存池
    nodes[node_count - 1]->pool_next = queue->pool_head;
    queue->pool_head = nodes[0];

    pthread_mutex_unlock(&queue->mutex);
}

// 销毁队列，释放资源（移除static）
void can_queue_destroy(CanFrameQueue* queue) {
    if (!queue) return;

    // 释放内存池
    if (queue->pool_head) {
        free(queue->pool_head);
        queue->pool_head = NULL;
    }

    // 销毁锁和条件变量
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->cond);

    // 清空队列
    queue->head = queue->tail = NULL;
    queue->count = 0;
    queue->max_size = 0;
}
