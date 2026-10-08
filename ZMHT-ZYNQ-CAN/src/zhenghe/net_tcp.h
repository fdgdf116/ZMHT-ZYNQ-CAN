#ifndef NET_TCP_H
#define NET_TCP_H

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

// -------------------------- 端口号定义 --------------------------
#define TCP_PORT1 9009    // 对应tcp_listen_thread_9009
#define TCP_PORT2 9010    // 对应tcp_listen_thread_9010
#define TCP_PORT3 9011    // 对应tcp_listen_thread_9011
#define TCP_PORT4 9012    // 对应tcp_listen_thread_9012

// -------------------------- 外部变量声明 --------------------------
// TCP监听线程ID数组（4个端口对应4个线程）
extern pthread_t listen_tid[4];

// -------------------------- 函数声明 --------------------------
/**
 * @brief 初始化TCP网络模块（创建4个端口的监听线程）
 * @return 0：初始化成功；-1：初始化失败
 */
int net_tcp_init(void);

/**
 * @brief 销毁TCP网络模块（回收监听线程资源）
 * @return 0：销毁成功；-1：销毁失败
 */
int net_tcp_destroy(void);

/**
 * @brief TCP端口9009监听线程（处理该端口的客户端连接与数据交互）
 * @param arg：传入的端口号（void* 类型，需强制转换为int*）
 * @return 线程退出时返回NULL
 */
void* tcp_listen_thread_9009(void* arg);

/**
 * @brief TCP端口9010监听线程（功能同9009，对应不同端口）
 * @param arg：传入的端口号（void* 类型，需强制转换为int*）
 * @return 线程退出时返回NULL
 */
void* tcp_listen_thread_9010(void* arg);

/**
 * @brief TCP端口9011监听线程（功能同9009，对应不同端口）
 * @param arg：传入的端口号（void* 类型，需强制转换为int*）
 * @return 线程退出时返回NULL
 */
void* tcp_listen_thread_9011(void* arg);

/**
 * @brief TCP端口9012监听线程（功能同9009，对应不同端口）
 * @param arg：传入的端口号（void* 类型，需强制转换为int*）
 * @return 线程退出时返回NULL
 */
void* tcp_listen_thread_9012(void* arg);

#endif  // NET_TCP_H