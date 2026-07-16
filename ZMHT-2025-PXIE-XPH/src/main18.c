#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <poll.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <stdbool.h>

#include "axican.h"
#include "can_frame_queue.h"
#include "ringbuffer_channels.h"
#include "zmuav_pl2ps_irq.h"

// 配置监听的四个端口
#define TCP_PORT1 9009   // 命令下发端口
#define TCP_PORT2 9010   // 数据上传端口
#define TCP_PORT3 9011   // 事件/错误上报端口
#define TCP_PORT4 9012   // 上位机下发数据端口
#define DATA_DOWNLOAD_PORT TCP_PORT4

#ifndef PPS_IRQ_DEV_PATH
#define PPS_IRQ_DEV_PATH "/dev/zmuav_pl2ps_irq_2"
#endif

//正常下发数据模式
#define NORMAL_MODE 0X0000
#define ALONG_MODE 0X0001
#define INIT_MODE  0X0002

// 广播调度模式
#define BCAST_MODE_SIMULTANEOUS 1
#define BCAST_MODE_POLLING      2
#define BCAST_MODE_AB_ALTERNATE 3
#define BCAST_ENABLE            1
#define BCAST_CAN_MASK          0x3FU
#define BCAST_MAX_FRAMES_PER_GROUP 64
#define BCAST_GROUP_QUEUE_DEPTH 32
#define BCAST_CAN_WRITE_TIMEOUT_MS 5

#define MAX_CLIENTS 5
#define BUFFER_SIZE 4096
#define BUFFER_SIZE_1 1048628
#define SIMULATOR_BUFFER_SIZE 1000  // 单机模拟缓冲区大小
//启动和关闭CAN功能
#define OPEN_CAN_DEVICE 0xAA
#define CLOSE_CAN_DEVICE 0xBB

// 同步字
#define REQ_SYNC_WORD 0x03CCF0FFUL
#define ACK_SYNC_WORD 0x05CCF0FFUL
#define EVNET_SYNC_WORD  0x02CCF0FF
#define PC_ARM_DATA_WORD 0x04CCF0FF
#define PC_STATUS_HEAD 0X499602D2
#define PC_ARM_STATUS_WORD 0xFFF0CC01

//宏定义
#define CAN_FIFO_TWO_FRAME_SIZE (16)
#define CAN_FIFO_THR_FRAME_SIZE  (4)
#define DEQUEUE_BATCH_SIZE 32
#define DEQUEUE_BATCH_SIZE_1 10
#define CAN_QUEUE_MAX_SIZE 10000
#define POLL_TIMEOUT_MS -1
#define CAN_FRAME_DATA_LEN 16
//用于分包校验是否连续，如果不连续则丢弃，连续则发送
// 定义最大分包数量
#define MAX_PACKETS 32
// 分包超时时间(毫秒)
#define PACKET_TIMEOUT 1000
//增加一个线程定时往9010端口上传数据，且保证数据安全，线程上锁
static pthread_t buf_report_tid;

int Can_Byte=0;


uint32_t Can_TxFrame_t[4];

// 全局变量
static pthread_mutex_t event_sock_mutex = PTHREAD_MUTEX_INITIALIZER;
static int event_socket = -1;
static uint16_t event_counter = 0;
static pthread_mutex_t data_upload_mutex = PTHREAD_MUTEX_INITIALIZER;
static int data_upload_socket = -1;
static uint16_t data_counter = 0;
static pthread_mutex_t data_counter_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint16_t ch_counter = 0;
static pthread_mutex_t common_data_mutex = PTHREAD_MUTEX_INITIALIZER;
static int common_data = -1; 

uint32_t channel_frame_count[6] = {0};
static volatile int running = 1;

//单机模拟计数器（判断需要多少次中断）
static pthread_mutex_t along_counter_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t along_counter_cond = PTHREAD_COND_INITIALIZER;
static uint32_t along_more_int_couter=0;
//9012端口互斥锁
static pthread_mutex_t data_download_mutex = PTHREAD_MUTEX_INITIALIZER;
static int data_download_socket = -1;
//分包互斥锁

// 分包缓冲区互斥锁
static pthread_mutex_t packet_mutex = PTHREAD_MUTEX_INITIALIZER;



// 事件ID枚举
typedef enum {
    EVENT_CAN_INIT_FAILED = 0x0001, //模块初始化失败
    EVENT_CAN_SENT_FAILE  = 0X0002, //发送失败
} EventID;
typedef enum {
    EVENT_LEVEL_INFO  = 0x0000, //风险
    EVENT_LEVEL_RISK  = 0x0001, //提示
    EVENT_LEVEL_ERROR = 0x0002, //错误
    EVENT_LEVEL_FATAL = 0x0003, //系统挂起
} EventLevel;

// 事件包结构
#pragma pack(push, 1)
typedef struct {
    uint32_t sync_word;
    uint32_t data_len;
    EventLevel event_level;
    EventID event_id;
    uint16_t timestamp;
    uint16_t checksum;
    char description[64];
} EventPacket;
#pragma pack(pop)

//CAN设备定义
typedef enum {
    AXICAN0 = 0,
    AXICAN1,
    AXICAN2,
    AXICAN3,
    AXICAN4,
    AXICAN5,
    AXICAN_MAX
} axican_drv_t;
#define MAX_CHANNELS 32          // 通道数（CAN/DMA/1553B 共用）
#define MAX_TYPES    DATA_TYPE_MAX

struct axican axican_test[MAX_CHANNELS];
static const char *axican_drv_name[] = {
    "/dev/axican_0x83c00000",
    "/dev/axican_0x83c10000", 
    "/dev/axican_0x83c20000", 
    "/dev/axican_0x83c30000",
    "/dev/axican_0x83c40000",
    "/dev/axican_0x83c50000"
};

// CAN状态枚举
typedef enum {
    CAN_STATE_CLOSED = 0,
    CAN_STATE_OPENED = 1
} CanState;

// CAN属性配置指令结构体
#pragma pack(push, 1)
typedef struct {
    uint32_t head;
    uint8_t direction;
    uint8_t type;
    uint8_t length;
    uint8_t ch_id;
    uint32_t total_count;
    uint32_t ch_count;
    uint32_t baud_rate;
    uint8_t mode;
    uint8_t auto_send;
    uint16_t ier;
    uint32_t Packet_Interval;
    //uint8_t reserved[8];
    uint16_t CAN_DATA_MODE;//新加一个
    uint8_t reserved[6];
    uint64_t timestamp;
    uint32_t xor_checksum;
} CanConfigParam;
#pragma pack(pop)

// 广播调度配置命令结构体（9009端口，命令0x0104）
#pragma pack(push, 1)
typedef struct {
    uint32_t head;
    uint8_t direction;
    uint8_t type;
    uint8_t length;
    uint8_t reserved;
    uint32_t config_count;
    uint8_t enable;
    uint8_t b_mode;
    uint8_t start_group;
    uint8_t start_channel;
    uint32_t active_mask;
    uint32_t group_a_mask;
    uint32_t group_b_mask;
    uint64_t timestamp;
    uint32_t xor_checksum;
} BroadcastConfigParam;
#pragma pack(pop)

typedef struct {
    uint8_t enable;
    uint8_t mode;
    uint8_t start_group;
    uint8_t current_group;
    uint8_t start_channel;
    uint8_t poll_next_channel;
    uint32_t active_mask;
    uint32_t group_a_mask;
    uint32_t group_b_mask;
    uint32_t config_count;
} BroadcastSchedulerConfig;

typedef struct {
    uint32_t group_id;
    uint16_t frame_count;
    struct axican_frame frames[BCAST_MAX_FRAMES_PER_GROUP];
} BroadcastFrameGroup;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    uint32_t pending;
} BroadcastChannelWake;

// 记录CAN配置的结构体
typedef struct {
    uint8_t can_id;       //can id
    uint32_t baud_rate;   //波特率
    uint8_t mode;         //模式正常还是回环
    uint8_t can_type;     //正常数据流还是单机模拟
    uint8_t valid;        //是否配置有效
    CanState state;       //设备是否开启
} CanDeviceConfig;

// CAN信息查询请求结构
typedef struct {
    uint8_t can_id;
    uint32_t baud_rate;
    uint8_t mode;
    uint8_t can_type;
} CanQueryParam;

// CAN信息查询应答结构
typedef struct {
    uint8_t can_id;
    uint32_t baud_rate;
    uint8_t mode;
    uint8_t can_type;
} CanQueryResp;

// CAN信息查询应答包
#pragma pack(push, 1)
typedef struct {    
    uint32_t CAN_DATA_RE_HEAD;
    uint32_t CAN_DATA_RE_LENGTH;
    uint16_t CAN_DATA_RE_COUNT;
    uint16_t CAN_DATA_RE_COMMOND;
    uint32_t CAN_DATA_RE_Checksum;
    //CanConfigParam CAN_RE_DATA;
    uint8_t CAN_RE_DATA[];
} CAN_Send_Response_r;
#pragma pack(pop)

// CAN消息结构体
typedef struct {
    uint32_t IDR;
    uint32_t DRCR;
    uint32_t DW1R;
    uint32_t DW2R;
} CAN_Message;

typedef struct {
	unsigned int    can_id;
	unsigned int   	can_dlc;
	unsigned int   	data[2];
	long long		timestamp;
}CAN_RECE_MESSAGE;

// CAN数据协议结构体
typedef struct {
    uint8_t  Ch_Count;
    uint8_t  Type;
    uint8_t  Direction;
    uint32_t Frame_Count;
    CAN_Message msg;
    uint16_t Packets_Sum;
    uint16_t Packets_No;
    uint64_t Timestamp;
} CAN_Data_Protocol;

// CAN启动和关闭功能应答帧
typedef struct {    
    uint32_t CAN_DATA_RE_HEAD;
    uint32_t CAN_DATA_RE_LENGTH;
    uint16_t CAN_DATA_RE_COUNT;
    uint16_t CAN_DATA_RE_COMMOND;
    uint32_t CAN_DATA_RE_Checksum;
    uint32_t CAN_DATA_RE_STATUS;
} CAN_Send_Response;

// 缓冲区查询请求参数
typedef struct {
    uint32_t buf_type;
} CanBufQueryParam;
#pragma pack(push, 1)
// 缓冲区单通道查询数据
typedef struct {
    // uint32_t CAN_DATA_RE_HEAD;
    // uint32_t CAN_DATA_RE_LENGTH;
    // uint16_t CAN_DATA_RE_COUNT;
    // uint16_t CAN_DATA_RE_COMMOND;
    // uint32_t CAN_DATA_RE_Checksum;
    uint8_t channel;                          // 通道标识 (1字节)s
    uint32_t normal_buf_remaining;            // 正常数据缓冲区剩余容量 (4字节)
    uint32_t pulse_buf_remaining;             // 复用为广播组缓冲区剩余组数 (4字节)
    uint32_t sim_buf_remaining;               // 单机模拟器数据缓冲区剩余容量 (4字节)
    uint32_t dmaddr_space;
    uint32_t vfifo_space;
    uint32_t axififo_space ;
} CanBufRemainingResponse;
#pragma pack(pop)

// 多通道缓冲区查询缓冲数据
#pragma pack(push, 1)
typedef struct {
    uint32_t tcp_buffer_head;                 //定时上传同步头
    uint32_t tcp_buffer_length;
    uint16_t tcp_buffer_count;
    uint16_t tcp_buffer_commond;
    uint32_t tcp_buffer_checksum;
    CanBufRemainingResponse channels[MAX_CHANNELS];    // 32个通道的信息
} MultiChannelBufResponse;
#pragma pack(pop)

// 缓冲区查询应答包结构
typedef struct {    
    uint32_t CAN_DATA_RE_HEAD;
    uint32_t CAN_DATA_RE_LENGTH;
    uint16_t CAN_DATA_RE_COUNT;
    uint16_t CAN_DATA_RE_COMMOND;
    uint32_t CAN_DATA_RE_Checksum;
    CanBufRemainingResponse CAN_RE_DATA;
} CAN_Buf_Query_Response;
#pragma pack(push, 1) 
// 上位机下发数据协议格式
typedef struct {
    uint32_t head;
    uint8_t direction;
    uint8_t type;
    uint8_t length;
    uint8_t ch_id;
    uint32_t total_count;
    uint32_t ch_count;
    uint32_t idr;
    uint32_t drcr;
    uint32_t dw1r;
    uint32_t dw2r;
    uint16_t packets_sum;
    uint16_t packets_no;
    uint64_t timestamp;
    uint32_t xor_checksum;
} PC_ARM_DATA_DATA;
#pragma pack(pop)
#pragma pack(push, 1)  
// arm返回状态结构体
typedef struct {
    uint32_t PC_arm_head;
    uint32_t PC_arm_length;
    uint16_t PC_arm_conter;
    uint16_t PC_arm_commond;
    uint32_t pc_arm_check;
    uint32_t pc_arm_chind;
    uint32_t head;
    uint8_t direction;
    uint8_t type;
    uint8_t length;
    uint8_t ch_id;
    uint32_t total_count;
    uint32_t ch_count;
    uint32_t idr;
    uint32_t drcr;
    uint32_t dw1r;
    uint32_t dw2r;
    uint8_t states;
    uint8_t xor_states;
    uint16_t reserved2;
    uint64_t timestamp;
    uint32_t xor_checksum;
} ARM_PC_STATUS_DATA;
#pragma pack(pop)
// 单机模拟结构体
#pragma pack(push, 1) 
typedef struct {
    uint32_t head;
    uint8_t direction;
    uint8_t type;
    uint16_t length;
    uint8_t channel_id;
    uint32_t total_count;
    uint32_t channel_count;
    uint16_t packet_type;
    uint32_t can_id;
    uint32_t data_type;
    uint16_t rcv_len;
    uint16_t send_len;
    uint8_t reserved;
    uint8_t send_data[256];
} PC_ARM_SIMULATOR_DATA;
#pragma pack(pop)

//1553B协议格式
#pragma pack(push, 1) 
typedef struct{
    uint32_t PC_OFFSET_1553;  //偏移量
    uint16_t PC_LENGTH_1553;  //长度
    uint8_t PC_DATA[128];     //数据

}PC_1553_DATA;
#pragma pack(pop)

//DMA协议格式
#pragma pack(push, 1) 
typedef struct
{
    uint8_t DMA_CHINNEL;
    uint8_t DMA_REMAIN ;
}PC_DMA_DATA;
#pragma pack(pop)

//秒脉冲数据结构体
#pragma pack(push, 1)
typedef struct {
    uint64_t timestamp;  // 精确时间戳
    uint8_t pulse_flag;  // 脉冲标志
} SecondPulseData;
#pragma pack(pop)

// 上位机下发协议
#pragma pack(push, 1)
// 主协议结构体，包含不同模式的数据
typedef struct {
    uint32_t PC_ARM_DATE_HEAD;      // 协议头部
    uint32_t PC_ARM_DATE_LENGTH;    // 数据长度
    uint16_t PC_ARM_DATE_COUNTER;   // 数据包计数器
    uint16_t PC_ARM_DATE_COMMAND;   // 命令类型，用于区分协议格式
    uint32_t PC_ARM_DATE_CHECKSUM;  // 主校验和
    uint32_t PC_ARM_DATE_CANID;     // CAN ID
    //如果收到的协议不行的话，可以将共用体改为这种 uint8_t  data[];        // 变长数据
   // uint8_t  data[];  //存储数据
    union {
        PC_ARM_DATA_DATA normal_data;      // 正常模式数据
        PC_ARM_SIMULATOR_DATA sim_data;    // 单机模拟模式数据
    } data;  // 使用联合体存储不同模式的数据
} Can_PC_ARM_DATA;
#pragma pack(pop)



typedef enum {
    DATA_TYPE_CAN = 0,       // CAN帧数据
    DATA_TYPE_SECOND_PULSE,  // 秒脉冲数据
    DATA_TYPE_SIMULATOR ,     // 单机模拟数据
    DATA_TYPE_DMA,            //DMA数据
    DATA_TYPE_1553B,          //1553B数据
    DATA_TYPE_MAX               // 总类型数
} DataType;

typedef struct {
    DataType data_type;  // 类型标识：当前存储的是哪种结构体
    union {              // 联合体：容纳所有可能的结构体（大小=最大结构体的大小）
        PC_ARM_DATA_DATA can_msg;
        SecondPulseData pulse_data;
        PC_ARM_SIMULATOR_DATA sim_data;
        PC_1553_DATA pc_1553_data;
        PC_DMA_DATA  pc_dma_data;
    } data;              // 实际存储的数据
} GenericDataFrame;  // 通用数据帧
typedef struct {
    GenericDataFrame *frames;  // 改为存储通用帧
    uint32_t max_frames;       // 最大帧数（不变）
    uint32_t total_size;       // 总字节数（根据通用帧计算）
    uint32_t used_size;        // 已用字节数（不变）
    uint32_t frame_count;      // 已存帧数（不变）
    uint8_t overflow_flag;     // 溢出标志（不变）
    pthread_mutex_t mutex;                       // 缓存操作互斥锁
    pthread_cond_t cond;                         // 缓存有数据时唤醒读线程
} CanBufferState;
// 缓冲区元数据：每个“通道-数据类型”对应一个实例
typedef struct BufferMeta {
    // 1. 唯一标识
    uint8_t   channel;          // 所属通道（0~31）
    DataType  data_type;        // 数据类型（0~4）
    
    // 2. 动态帧数配置
    size_t    frame_size;       // 单帧大小（=对应数据结构体大小）
    size_t    max_frames;       // 最大缓存帧数（动态配置）
    size_t    cur_frames;       // 当前已缓存帧数
    void*     data_area;        // 数据区（动态开辟：frame_size * max_frames）
    
    // 3. 同步与状态
    pthread_rwlock_t rw_lock;   // 读写锁（读并发，写独占）
    time_t    last_update;      // 最后更新时间（超时清理用）
    uint32_t  overflow_count;   // 溢出次数统计
    int       is_valid;         // 缓冲区有效性标志（0=无效，1=有效）
    
    // 4. 链表节点（用于全局遍历/清理）
    struct BufferMeta* prev;
    struct BufferMeta* next;
} BufferMeta;
//全局缓冲区管理器（整合所有资源）
typedef struct BufferManager {
    BufferMeta* buf_array[MAX_CHANNELS][DATA_TYPE_MAX ]; // 二维数组：快速索引"通道-数据类型"
    BufferMeta* list_head;                               // 链表头（全局遍历）
    BufferMeta* list_tail;                               // 链表尾（全局遍历）
    pthread_mutex_t list_lock;                           // 链表操作锁
    int          is_running;                             // 管理器运行标志
} BufferManager;
#pragma pack(push, 1)  
//ARM接收CAN数据组帧返回给上位机
typedef struct {
    uint32_t PC_ARM_DATE_HEAD;      // 协议头部
    uint32_t PC_ARM_DATE_LENGTH;    // 数据长度
    uint16_t PC_ARM_DATE_COUNTER;   // 数据包计数器
    uint16_t PC_ARM_DATE_COMMAND;   // 命令类型，用于区分协议格式
    uint32_t PC_ARM_DATE_CHECKSUM;  // 主校验和
    uint32_t PC_ARM_CHIND;

    uint32_t head;
    uint8_t direction;
    uint8_t type;
    uint8_t length;
    uint8_t ch_id;
    uint32_t total_count;
    uint32_t ch_count;
    uint32_t idr;
    uint32_t drcr;
    uint32_t dw1r;
    uint32_t dw2r;
    uint8_t status;
    uint8_t reserved1;
    uint16_t reserved2;
    uint64_t timestamp;
    uint32_t xor_checksum;
}CAN_ARM_PC_DATA;
#pragma pack(pop)

//启动和关闭设备
typedef struct {
    uint32_t can_id;
    uint32_t state;
} CanStartStopParam;

// 命令类型枚举
typedef enum {
    CMD_CAN_BUF_QUERY = 0x0100,
    CMD_CAN_CONFIG = 0x0101,
    CMD_CAN_INFO_QUERY = 0x0102,
    CMD_CAN_START_STOP = 0x0103,
    CMD_CAN_BROADCAST_CONFIG = 0x0104
} CmdType;

// 命令请求包结构
#pragma pack(push, 1)  
typedef struct {
    uint32_t sync_word;
    uint32_t data_len;
    uint16_t counter;
    uint16_t cmd_type;
    uint32_t checksum;
    uint32_t channel;
    uint8_t data[];
} DATA_ReqPacket;
#pragma pack(pop)
#pragma pack(push, 1)  
typedef struct {
    uint32_t sync_word;
    uint32_t data_len;
    uint16_t counter;
    uint16_t cmd_type;
    uint32_t checksum;
    uint8_t data[];
} ReqPacket;
#pragma pack(pop)
// 解析状态枚举
typedef enum {
    PARSE_SUCCESS,
    PARSE_ERR_SYNC,
    PARSE_ERR_LEN,
    PARSE_ERR_CHECKSUM
    
} ParseStatus;
//用来存储接收到的CAN帧
typedef struct {
    uint32_t bytes[4];  // 存储4字节数据
} CanFourByteStruct;
//用于分包校验是否连续，如果不连续则丢弃，连续则发送
typedef struct {
    PC_ARM_DATA_DATA frames[DEQUEUE_BATCH_SIZE_1];  // 存储多包数据
    uint8_t total_packets;                      // 总包数
    uint8_t received_packets;                   // 已接收包数
    bool is_active;                             // 缓冲区是否激活（等待多包数据）
    time_t last_receive_time;                   // 最后一包接收时间（超时判断）
} PacketBuffer;


//全局缓存队列，为每个CAN通道创建一个队列
CanFrameQueue can_queues[AXICAN_MAX];
CanFrameQueue can_queues_1[AXICAN_MAX];

// 全局变量初始化
static BufferManager g_buf_mgr; //全局管理器实例（程序唯一）
CanDeviceConfig can_configs[AXICAN_MAX] = {0};
CanQueryResp resp[AXICAN_MAX];
CanBufferState can_buffers[MAX_CHANNELS][DATA_TYPE_MAX];
static PacketBuffer packet_buffers[AXICAN_MAX] = {0};

uint8_t g_temp_buf[BUFFER_SIZE_1 * 2];  // 暂存不完整数据包（最大支持2个BUFFER_SIZE长度）
uint32_t g_temp_len = 0; // 暂存区当前数据长度
uint8_t  can_data_type=0;            

static uint32_t g_tcp_buffer_total = BUFFER_SIZE_1;  // 初始值为BUFFER_SIZE
static uint32_t g_tcp_buffer_used = 0;
static pthread_mutex_t tcp_buffer_mutex = PTHREAD_MUTEX_INITIALIZER;

RingBuffer32ChSet g_simulator_buf_set;
RingBuffer32ChSet g_data_buf_set;
RingBuffer32ChSet g_pulse_buf_set;
//创建队列结构体的数据
RingBuffer32ChSet g_can_buf_set;
//创建监控队列结构 
RingBuffer32ChSet g_poll_buf_set;
// 每个CAN通道的秒脉冲广播组缓冲区
RingBuffer32ChSet g_bcast_group_buf_set;

static BroadcastSchedulerConfig g_bcast_cfg = {0};
static pthread_mutex_t bcast_cfg_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t can_write_mutex[AXICAN_MAX];
static BroadcastChannelWake g_bcast_wake[AXICAN_MAX];
static uint32_t g_bcast_group_id = 0;
static pthread_mutex_t bcast_group_id_mutex = PTHREAD_MUTEX_INITIALIZER;

// 函数声明
// static uint8_t calculate_checksum(const uint8_t *data, uint32_t len);
static void process_download_data(const uint8_t *buffer, size_t len);
static uint32_t calculate_xor_checksum(const uint8_t *data, uint32_t len);
static void send_received_can_frame_to_pc(uint8_t can_id, const struct axican_frame *recv_frame);
static void process_simulator_mode_data(uint8_t can_id, PC_ARM_SIMULATOR_DATA *data);
static void handle_normal_mode(int can_id, const struct axican_frame* frame);
static void handle_broadcast_config(int sock, ReqPacket *req);
static int process_9012_can_payload_frames(const uint8_t *payload, size_t payload_len, uint32_t packet_channel);
static void *pps_irq_thread(void *arg);
static void *broadcast_can_send_thread(void *parameter);

uint16_t reverse_2bytes(uint16_t value) {
    return ((value & 0x00FF) << 8) |  // 低8位移到高8位
           ((value & 0xFF00) >> 8);   // 高8位移到低8位
}

uint32_t reverse_4bytes(uint32_t value) {
    return ((value & 0x000000FF) << 24) |
           ((value & 0x0000FF00) << 8)  |
           ((value & 0x00FF0000) >> 8)  |
           ((value & 0xFF000000) >> 24);
}

// 发送事件到9011端口
static void send_event(EventLevel level, EventID id, int32_t error_code, const char *msg) {
    pthread_mutex_lock(&event_sock_mutex);
    
    if (event_socket == -1) {
        pthread_mutex_unlock(&event_sock_mutex);
        printf("No event client connected, cannot send event: %s\n", msg);
        return;
    }
    
    EventPacket event;
    memset(&event, 0, sizeof(event));
    
    // 填充事件包字段
    event.sync_word = EVNET_SYNC_WORD;
    event.data_len = sizeof(EventPacket);  // 数据总长度
    event.event_level = level;             // 事件级别
    event.event_id = id;                   // 事件ID
    
    // 获取时间戳（使用当前系统时间）
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    event.timestamp = (uint64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;  
    
    // 复制描述信息
    strncpy(event.description, msg, sizeof(event.description) - 1);
    
    // 计算校验和（不包含校验和字段本身）
    event.checksum = calculate_xor_checksum((uint8_t *)&event, sizeof(EventPacket) - sizeof(event.checksum));
    
    // 发送事件包
    ssize_t sent = send(event_socket, &event, sizeof(event), 0);
    if (sent != sizeof(event)) {
        perror("Failed to send event");
        close(event_socket);
        event_socket = -1;
    } else {
        printf("Sent event (level 0x%04X, id 0x%04X): %s\n", level, id, msg);
    }
    
    pthread_mutex_unlock(&event_sock_mutex);
}



// 辅助函数：根据匹配到的帧确定需要多少帧才回包
static int determine_response_frames(PC_ARM_SIMULATOR_DATA *frame)
{
    if (!frame) return 0;
    int re_conunt=0;
    re_conunt = (frame->rcv_len/8)-1;
    return re_conunt;
}

static int adjust_tcp_buffer_total(uint32_t new_total) {
    pthread_mutex_lock(&tcp_buffer_mutex);
    // 新总容量必须≥已使用量，否则可能导致已缓存数据丢失
    if (new_total < g_tcp_buffer_used) {
        pthread_mutex_unlock(&tcp_buffer_mutex);
        printf("[TCP Buffer] 调整失败：新总容量(%d) < 已使用量(%d)\n", new_total, g_tcp_buffer_used);
        return -1;
    }
    g_tcp_buffer_total = new_total;
    pthread_mutex_unlock(&tcp_buffer_mutex);
    //printf("[TCP Buffer] 总容量已调整为：%d 字节\n", new_total);
    return 0;
}


// 更新TCP缓冲区使用量的函数
static void update_tcp_buffer_usage(ssize_t bytes) {
    pthread_mutex_lock(&tcp_buffer_mutex);
    if (bytes > 0) {
        // 接收数据，增加已使用量
        g_tcp_buffer_used += bytes;
        if (g_tcp_buffer_used > BUFFER_SIZE) {
            g_tcp_buffer_used = BUFFER_SIZE;
        }
    } else if (bytes < 0) {
        // 处理数据，减少已使用量（假设bytes的绝对值为处理的数据量）
        uint32_t processed = (uint32_t)(-bytes);
        if (g_tcp_buffer_used >= processed) {
            g_tcp_buffer_used -= processed;
        } else {
            g_tcp_buffer_used = 0;
        }
    }
    pthread_mutex_unlock(&tcp_buffer_mutex);
}

//校验和
static uint32_t calculate_xor_checksum(const uint8_t *data, uint32_t len) {
    uint32_t xor_sum = 0;
    
    if (data == NULL || len == 0) {
        return xor_sum;
    }
    uint32_t res = 0;
    for (uint32_t i = 0; i < len; i += 4)
    {

        res ^= *(uint32_t*)(data + i);
       //printf("Byte %u (0x%08X), XOR sum: 0x%08X\n\r", i, data[i], res);
    }
    // for (uint32_t i = 0; i < len; i++) {
    //     xor_sum ^= data[i];
    //     printf("Byte %u (0x%02X), XOR sum: 0x%02X\n\r", i, data[i], xor_sum);
    // }
    
    return res;
   // return  xor_sum;
}

uint32_t swap_endian_32bit(uint32_t value) {
    return ((value & 0xFF000000) >> 24) |
           ((value & 0x00FF0000) >> 8)  |
           ((value & 0x0000FF00) << 8)  |
           ((value & 0x000000FF) << 24);
}




// 通过9010端口上传接收应答帧
static void send_received_can_frame_to_pc(uint8_t can_id, const struct axican_frame *recv_frame) {
    pthread_mutex_lock(&data_upload_mutex);
    if (data_upload_socket == -1) {
        pthread_mutex_unlock(&data_upload_mutex);
        printf("No data upload client (port %d) connected, skip sending received CAN frame\n", TCP_PORT2);
        return;
    }
    if (can_configs[can_id].state != CAN_STATE_OPENED) {
        pthread_mutex_unlock(&data_upload_mutex);
        printf("CAN%d is closed, skip sending received data\n", can_id);
        return;
    }
    
    
    ARM_PC_STATUS_DATA can_recv_frame;
    memset(&can_recv_frame, 0, sizeof(can_recv_frame));

    can_recv_frame.PC_arm_head = PC_ARM_STATUS_WORD;
    can_recv_frame.PC_arm_length = sizeof(ARM_PC_STATUS_DATA)-16;
    can_recv_frame.PC_arm_conter=0;
    can_recv_frame.PC_arm_commond=0;
    can_recv_frame.pc_arm_check = 0;
    
    can_recv_frame.head = PC_STATUS_HEAD;
    can_recv_frame.direction = 0x01;
    can_recv_frame.type = 0x81;
    can_recv_frame.length = sizeof(ARM_PC_STATUS_DATA);
    can_recv_frame.ch_id = can_id;
    
    uint32_t can_data_dw1 = 0, can_data_dw2 = 0;
    memcpy(&can_data_dw1, recv_frame->data, 4);
    memcpy(&can_data_dw2, recv_frame->data + 4, 4);
    can_recv_frame.dw1r = can_data_dw1;
    can_recv_frame.dw2r = can_data_dw2;
    can_recv_frame.idr = recv_frame->can_id;
    can_recv_frame.drcr = recv_frame->can_dlc;
    
    can_recv_frame.states = 0x00;
    can_recv_frame.xor_states = can_recv_frame.states ^ 0xFF;
    can_recv_frame.timestamp = recv_frame->timestamp;
    
    uint32_t temp_checksum = can_recv_frame.xor_checksum;
    can_recv_frame.xor_checksum = 0;
    can_recv_frame.xor_checksum = calculate_xor_checksum(
        (uint8_t *)&can_recv_frame, 
        sizeof(ARM_PC_STATUS_DATA) - sizeof(can_recv_frame.xor_checksum)
    );
    
    ssize_t sent_len = send(data_upload_socket, &can_recv_frame, sizeof(can_recv_frame), 0);
    if (sent_len != sizeof(can_recv_frame)) {
        perror("Failed to send received CAN frame to PC (port 9010)");
        close(data_upload_socket);
        data_upload_socket = -1;
        send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,"Send CAN frame to PC failed (port 9010), close invalid socket" );
    } else {
        printf("Sent received CAN frame to PC (port %d): CAN%d, ID=0x%08X, DLC=%d\n",
               TCP_PORT2, can_id, recv_frame->can_id, recv_frame->can_dlc);
    }
    
    pthread_mutex_unlock(&data_upload_mutex);
}

// 通过9010端口上传CAN配置应答帧
static void send_can_config_response(uint8_t can_id, uint32_t status) {
    pthread_mutex_lock(&data_upload_mutex);
    if (data_upload_socket == -1) {
        pthread_mutex_unlock(&data_upload_mutex);
        printf("No data client connected on port %d, cannot send config response\n", TCP_PORT2);
        return;
    }
    
    CAN_Send_Response response;
    memset(&response, 0, sizeof(response));
    data_counter++;
    uint32_t req_sync_length = reverse_4bytes(sizeof(response));
    response.CAN_DATA_RE_HEAD = reverse_4bytes(ACK_SYNC_WORD);
    response.CAN_DATA_RE_LENGTH = req_sync_length;
    response.CAN_DATA_RE_COUNT = data_counter;
    response.CAN_DATA_RE_COMMOND = htons(0x0101);
    
    response.CAN_DATA_RE_Checksum = calculate_xor_checksum(
        (const uint8_t*)&response, 
        sizeof(response) - sizeof(response.CAN_DATA_RE_Checksum)
    );
    response.CAN_DATA_RE_STATUS = status;
    // printf("receive config head response:0x%08x\n\r",response.CAN_DATA_RE_HEAD);
    // printf("receive config  length response:0x%08x\n\r",response.CAN_DATA_RE_LENGTH);
    // printf("receive config counter response:0x%08x\n\r",response.CAN_DATA_RE_COUNT);
    // printf("receive config  commond response:0x%04x\n\r",response.CAN_DATA_RE_COMMOND);
    // printf("receive config checksum response:0x%08x\n\r",response.CAN_DATA_RE_Checksum);
    // printf("receive config status response:0x%08x\n\r",response.CAN_DATA_RE_STATUS);
    
    ssize_t sent = send(data_upload_socket, &response, sizeof(response), 0);
    if (sent != sizeof(response)) {
        perror("Failed to send CAN config response");
        close(data_upload_socket);
        data_upload_socket = -1;
    } else {
        printf("Sent CAN config response via port %d: CAN ID=%d, Status=%d\n",
               TCP_PORT2, can_id, status);
    }
    
    pthread_mutex_unlock(&data_upload_mutex);
}

// 通过9010端口上传CAN信息查询应答
static void send_can_info_response(CanConfigParam *devices, uint8_t total_count) {
    pthread_mutex_lock(&data_upload_mutex);
    if (data_upload_socket == -1) {
        pthread_mutex_unlock(&data_upload_mutex);
        printf("No data client connected on port %d, cannot send info response\n", TCP_PORT2);
        return;
    }
    
    // 计算需要的大小 - 使用实际传入的total_count而非固定的AXICAN_MAX
    size_t response_size = sizeof(CAN_Send_Response_r) + (total_count * sizeof(CanConfigParam));
    printf("CAN SEND: %d,config:%d,sizeof(CanConfigParam):%d",sizeof(CAN_Send_Response_r),(total_count * sizeof(CanConfigParam)),sizeof(CanConfigParam));
    // 开辟空间，分配内存
    CAN_Send_Response_r *response = malloc(response_size);
    if (!response) {
        printf("Failed to allocate memory for response");
        pthread_mutex_unlock(&data_upload_mutex);
        return;
    }
    memset(response, 0, response_size);
    
    // 填充公共头
    response->CAN_DATA_RE_HEAD = reverse_4bytes(ACK_SYNC_WORD);
    response->CAN_DATA_RE_LENGTH = reverse_4bytes(response_size);
    
    pthread_mutex_lock(&data_counter_mutex);
    data_counter++;
    response->CAN_DATA_RE_COUNT = data_counter;
    pthread_mutex_unlock(&data_counter_mutex);
    
    response->CAN_DATA_RE_COMMOND = reverse_2bytes(0x0102);
    // 使用实际数量复制数据，避免越界
    memcpy(response->CAN_RE_DATA, devices, total_count * sizeof(CanConfigParam));
    
    // 修正校验和计算 - 使用正确的大小和指针
    response->CAN_DATA_RE_Checksum = calculate_xor_checksum(
        (uint8_t *)response, 
        response_size - sizeof(response->CAN_DATA_RE_Checksum)
    );
    
    // 打印发送的数据内容（调试用）
    printf("=== 发送的CAN响应数据 ===\n");
    printf("头部: 0x%X\n", response->CAN_DATA_RE_HEAD);
    printf("长度: %zu\n", response->CAN_DATA_RE_LENGTH);
    printf("计数器: %d\n", response->CAN_DATA_RE_COUNT);
    printf("命令: 0x%X\n", response->CAN_DATA_RE_COMMOND);
    printf("校验和: 0x%X\n", response->CAN_DATA_RE_Checksum);
    printf("设备数量: %d\n", total_count);
    printf("========================\n");
    
    // 修正发送数据 - 使用正确的缓冲区指针和大小
    ssize_t sent = send(data_upload_socket, response, response_size, 0);
    if (sent == -1) {
        perror("Failed to send CAN info response frame");
        close(data_upload_socket);
        data_upload_socket = -1;
    } else if (sent != response_size) {
        fprintf(stderr, "Incomplete send: %zd of %zu bytes sent\n", sent, response_size);
        close(data_upload_socket);
        data_upload_socket = -1;
    } else {
        printf("Sent CAN info response with %d devices via port %d\n", total_count, TCP_PORT2);
    }  
    
    // 释放分配的内存，避免内存泄漏
    free(response);
    pthread_mutex_unlock(&data_upload_mutex);
}

// 通过9010端口上传CAN应答数据
static void send_can_send_response(CAN_Send_Response *response) {
    pthread_mutex_lock(&data_upload_mutex);
    if (data_upload_socket == -1) {
        pthread_mutex_unlock(&data_upload_mutex);
        printf("No data client connected on port %d, cannot send CAN send response\n", TCP_PORT2);
        return;
    }
    
    uint32_t total_len = sizeof(CAN_Send_Response);
    
    ssize_t sent = send(data_upload_socket, response, total_len, 0);
    if (sent != total_len) {
        perror("Failed to send CAN send response");
        close(data_upload_socket);
        data_upload_socket = -1;
    } else {
        printf("Sent CAN send response via port %d: Status=%d, Command=0x%X\n",
               TCP_PORT2, response->CAN_DATA_RE_STATUS, response->CAN_DATA_RE_COMMOND);
    }
    
    pthread_mutex_unlock(&data_upload_mutex);
}

// 下位机返回给上位机状态函数
static void send_can_transmit_response(uint8_t can_id,uint8_t type, uint8_t status,struct axican_frame *frame) {
    pthread_mutex_lock(&data_upload_mutex);
    //判断9010端口是否还在链接
    if (data_upload_socket == -1) {
        pthread_mutex_unlock(&data_upload_mutex);
        printf("No data client connected, cannot send transmit response\n");
        return;
    }
    
    ARM_PC_STATUS_DATA response;
    memset(&response, 0, sizeof(response));
    
    response.PC_arm_head = PC_ARM_STATUS_WORD;
    response.PC_arm_length = reverse_4bytes(sizeof(ARM_PC_STATUS_DATA)-20);
    response.PC_arm_conter=0;
    response.PC_arm_commond=0;
    response.pc_arm_check = 0;
    response.pc_arm_chind =reverse_4bytes(can_id);


    response.head = PC_STATUS_HEAD;
    response.direction = 1;
    response.type = type;
    response.length = 0x30;
    response.ch_id = can_id;
    response.total_count = (data_counter == 0xFFFFFFFF) ? 0 : data_counter + 1;
    channel_frame_count[response.ch_id] = (channel_frame_count[response.ch_id] == 0xFFFFFFFF) ? 0 : channel_frame_count[response.ch_id] + 1;
    response.ch_count = channel_frame_count[response.ch_id];
    response.idr=frame->can_id;
    response.drcr=frame->can_dlc;
    response.dw1r =frame->data[0];
    response.dw2r = frame->data[1];
    response.states = status;
    response.xor_states = status ;
    response.reserved2 = 0;
    
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    response.timestamp = (uint64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
    response.xor_checksum=calculate_xor_checksum((uint8_t *)&response, sizeof(ARM_PC_STATUS_DATA) - 4);
    
    ssize_t sent = send(data_upload_socket, &response, sizeof(response), 0);
    if (sent != sizeof(response)) {
        perror("Failed to send transmit response");
        close(data_upload_socket);
        data_upload_socket = -1;
    } else {
    }
    
    pthread_mutex_unlock(&data_upload_mutex);
}
void destroy_all_ring_buffers(void) {
    // 销毁核心数据环形缓冲区
    ringbuffer_32ch_destory(&g_data_buf_set, 32);
    
    // 销毁模拟器数据环形缓冲区
    ringbuffer_32ch_destory(&g_simulator_buf_set, 32);
    
    // 销毁秒脉冲数据环形缓冲区
    ringbuffer_32ch_destory(&g_pulse_buf_set, 32);
    
    // 销毁CAN接收数据队列缓冲区
    ringbuffer_32ch_destory(&g_can_buf_set, 6);  // 注意这里是6个通道（与初始化对应）
    
    // 销毁CAN监控数据队列缓冲区
    ringbuffer_32ch_destory(&g_poll_buf_set, 6);  // 6个通道（与初始化对应）

    // 销毁每通道广播组缓冲区
    ringbuffer_32ch_destory(&g_bcast_group_buf_set, 6);
    
    // 销毁CAN队列
    for (int i = 0; i < AXICAN_MAX; i++) {
        can_queue_destroy(&can_queues[i]);  // 假设存在对应的队列销毁函数
    }
    
    printf("所有环形缓冲区已销毁\n");
}

// 处理CAN配置命令
static void handle_can_config(int sock, ReqPacket *req) {
    uint32_t status = 0;
    const char *err_msg = NULL;
    int baud_success = 0;
    int mode_success = 0;
    
    CanConfigParam *param = (CanConfigParam *)req->data;
    if (param->ch_id >= AXICAN_MAX) {
        const char *err_msg = "Invalid CAN ID";
        send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
        return;
    }
    if(param->head != PC_STATUS_HEAD)
    {
        const char *err_msg = "DATA HEAND ERR";
        send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
        return ;
    }
    uint32_t expect_xor_checksum = calculate_xor_checksum((uint8_t *)param,sizeof(CanConfigParam)-4);
    if(param->xor_checksum != expect_xor_checksum)
    {
        const char *err_msg = "xor_checksum ERR";
        send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
        return ;
    }
    
    can_configs[param->ch_id].can_id = param->ch_id;
    can_configs[param->ch_id].valid = 1;
    can_configs[param->ch_id].can_type = param->CAN_DATA_MODE;
    printf(" CAN %d CAN TYPE CAN_DATA_MODE:0x%02x\n",param->ch_id,param->CAN_DATA_MODE);
    switch(param->CAN_DATA_MODE){
        case ALONG_MODE :
            if (param->type == 0x00) {
            
            uint32_t old_baud = can_configs[param->ch_id].baud_rate;
            uint8_t old_mode = can_configs[param->ch_id].mode;
            
            int ret = 0;
            // 1. 配置波特率
            if (param->baud_rate > 0 && param->baud_rate <= 1000000) {
                ret = axican_set_baud(axican_test[param->ch_id].f_rd.fd, param->baud_rate);
                if (ret != 0) {
                    const char *err_msg = "CAN set baud rate failed";
                    send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
                    baud_success = 0;
                }
                else
                {
                    can_configs[param->ch_id].baud_rate = param->baud_rate;
                    baud_success = 1;
                    printf("SET baud :%d baud_success:%d\n\r",param->baud_rate,baud_success);
                }      
            } else {
                
                const char *err_msg = "Invalid baud rate (0~1000000)";
                send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
            }
            switch (param->mode)
            {
            case ZMUAV_XCAN_MODE_NORMAL:
                    ret = axican_set_mode(axican_test[param->ch_id].f_rd.fd, ZMUAV_XCAN_MODE_NORMAL);
                    if (ret != 0) 
                    {
                        const char *err_msg = "CAN set mode failed";
                        send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
                        mode_success= 0;
                    }
                    else
                    {
                        mode_success=1;
                        printf("SET mode ZMUAV_XCAN_MODE_NORMAL baud_success:%d\n\r",mode_success);
                    }

                   
                break;
            case ZMUAV_XCAN_MODE_LOOPBACK:
                    ret = axican_set_mode(axican_test[param->ch_id].f_rd.fd, ZMUAV_XCAN_MODE_LOOPBACK);
                    if (ret != 0) 
                    {
                        const char *err_msg = "CAN set mode failed";
                        send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
                        mode_success= 0;
                    }
                    else
                    {
                        mode_success=1;
                        printf("SET mode ZMUAV_XCAN_MODE_LOOPBACK baud_success:%d\n\r",mode_success);
                    }

            default:
                    err_msg = "RECEIVE DATA MODE OTHER";
                    send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
                break;
            }

            
            status = (mode_success ==1 && baud_success == 1) ? 0 : 1;
            // printf("############status :%d\n\r",status);
            // printf("############mode_success:%d\n\r",mode_success);
            // printf("############baud_success:%d\n\r",baud_success);
            send_can_config_response(param->ch_id, status);
            break ;

        }
        case NORMAL_MODE :
            if (param->type == 0x00) 
            {
                
                uint32_t old_baud = can_configs[param->ch_id].baud_rate;
                uint8_t old_mode = can_configs[param->ch_id].mode;
                
                int ret = 0;
                // 1. 配置波特率
                if (param->baud_rate > 0 && param->baud_rate <= 1000000) {
                    ret = axican_set_baud(axican_test[param->ch_id].f_rd.fd, param->baud_rate);
                    if (ret != 0) {
                        const char *err_msg = "CAN set baud rate failed";
                        send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
                        baud_success=0;
                    }
                    can_configs[param->ch_id].baud_rate = param->baud_rate;
                    baud_success=1;
                    printf("SET baud :%d baud_success:%d\n\r",param->baud_rate,baud_success);
                } else {
                    const char *err_msg = "Invalid baud rate (0~1000000)";
                    send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
                }
            switch (param->mode)
            {
                case ZMUAV_XCAN_MODE_NORMAL:
                        ret = axican_set_mode(axican_test[param->ch_id].f_rd.fd, ZMUAV_XCAN_MODE_NORMAL);
                        if (ret != 0) 
                        {
                            const char *err_msg = "CAN set mode failed";
                            send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
                            mode_success= 0;
                        }
                        mode_success=1;
                         printf("SET mode ZMUAV_XCAN_MODE_NORMAL baud_success:%d\n\r",mode_success);
                        break;
                case ZMUAV_XCAN_MODE_LOOPBACK:
                        ret = axican_set_mode(axican_test[param->ch_id].f_rd.fd, ZMUAV_XCAN_MODE_LOOPBACK);
                        if (ret != 0) 
                        {
                            const char *err_msg = "CAN set mode failed";
                            send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
                            mode_success= 0;
                        }
                        mode_success=1;
                        printf("SET mode ZMUAV_XCAN_MODE_LOOPBACK baud_success:%d\n\r",mode_success);
                default:
                        err_msg = "RECEIVE DATA MODE OTHER";
                        send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
                        printf("SET mode ZMUAV_XCAN_MODE_NORMAL baud_success:%d\n\r",mode_success);
                    break;
            }
            status = (mode_success && baud_success) ? 0 : 1;
           // printf("############status :%d\n\r",status);
            send_can_config_response(param->ch_id, status);


            break;
            

            }
        default:
            err_msg = "Unsupported CAN data mode";
            send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
            status = 0;
            break;

    } 
}

// 处理CAN信息查询
static void handle_can_info_query(int sock, ReqPacket *req) {
    if (req->data != NULL) {
        // 2. 将 data 指针转为 uint32_t*，访问前 4 字节数据（注意大小端可能需要调整）
        uint32_t data_value = *(uint32_t *)req->data;
        // 3. 比较数据内容是否为 0xffffffff
        if (data_value != 0xffffffff) {
            const char *err_msg = "RECEIVE DATA NOT 0XFFFFFFFF";
            send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
            return;
        }
    }
    printf("######CAN 信息查询######\n");
    CanConfigParam can_responses[AXICAN_MAX];
    memset(can_responses, 0, sizeof(can_responses));
    
    for (uint8_t i = 0; i < AXICAN_MAX; i++) {
        CanConfigParam *can_mage_return_t = &can_responses[i];
        CanDeviceConfig *curr_cfg = &can_configs[i];
        can_mage_return_t->head = 0x499602D2;
        can_mage_return_t->direction = 0x1;
        can_mage_return_t->type = curr_cfg->can_type;
        can_mage_return_t->length = 0x30;
        can_mage_return_t->ch_id = curr_cfg->can_id;
        can_mage_return_t->total_count = (data_counter == 0xFFFFFFFF) ? 0 : data_counter + 1;
        channel_frame_count[curr_cfg->can_id] = (channel_frame_count[curr_cfg->can_id] == 0xFFFFFFFF) ? 0 : channel_frame_count[curr_cfg->can_id] + 1;
        can_mage_return_t->ch_count = channel_frame_count[curr_cfg->can_id];
        can_mage_return_t->baud_rate = can_configs->baud_rate;
        can_mage_return_t->mode = can_configs->mode;
        can_mage_return_t->auto_send = 0x1;
        can_mage_return_t->ier = 0x0000;
        can_mage_return_t->Packet_Interval = 0x00000000;
        can_mage_return_t->CAN_DATA_MODE =can_configs[curr_cfg->can_id].can_type;
        memset(can_mage_return_t->reserved, 0, sizeof(can_mage_return_t->reserved));
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        can_mage_return_t->timestamp = (uint64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
        can_mage_return_t->xor_checksum = calculate_xor_checksum((uint8_t *)&can_mage_return_t, sizeof(CanConfigParam) - 4);
    }
    
    send_can_info_response(can_responses, AXICAN_MAX);
    
    const char *ack_msg = "CAN info query processed, result sent via port 9010";
    send(sock, ack_msg, strlen(ack_msg), 0);
}
// 计算缓冲区剩余容量
static uint32_t calculate_buffer_remaining(CanBufferState *buf) {
    if (buf->frames == NULL) {
        return 0;
    }
    
    if (buf->used_size > buf->total_size) {
        return 0;
    }
    
    return buf->total_size - buf->used_size;
}

// 处理CAN缓冲区查询
static void handle_can_buf_query(int sock, ReqPacket *req) {
    CanBufQueryParam *param = (CanBufQueryParam *)req->data;
    //代表全部查询
    if (param->buf_type != 0xffffffff) {
        const char *err_msg = "Invalid buffer type (must be 1~3)";
        send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
        return;
    }
    //有6个应答帧
    MultiChannelBufResponse multi_response = {0};
    // multi_response.total_count = AXICAN_MAX;  // 固定为6个通道
    //上线程锁
    static pthread_mutex_t buffer_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&buffer_mutex);
    for (uint8_t ch = 0; ch < AXICAN_MAX; ch++)
    {
        if (ch < AXICAN_MAX) {
            multi_response.channels[ch].channel = ch;
            multi_response.channels[ch].normal_buf_remaining =
                ringbuffer_32ch_avail(&g_data_buf_set, ch, 32);
            multi_response.channels[ch].pulse_buf_remaining =
                ringbuffer_32ch_avail(&g_bcast_group_buf_set, ch, AXICAN_MAX);
            multi_response.channels[ch].sim_buf_remaining =
                ringbuffer_32ch_avail(&g_simulator_buf_set, ch, 32);
            multi_response.channels[ch].dmaddr_space = 0xFFFFFFFF;
            multi_response.channels[ch].vfifo_space = 0xFFFFFFFF;
            multi_response.channels[ch].axififo_space = 0xFFFFFFFF;
        } else {
            // 对于超出实际支持的通道，返回0
            multi_response.channels[ch].normal_buf_remaining = 0;
            multi_response.channels[ch].pulse_buf_remaining = 0;
            multi_response.channels[ch].sim_buf_remaining = 0;
        }
    }
    pthread_mutex_unlock(&buffer_mutex);
    pthread_mutex_lock(&data_upload_mutex);
    if (data_upload_socket == -1) {
        pthread_mutex_unlock(&data_upload_mutex);
        printf("No data client for buffer query response\n");
        return;
    }
    
    ssize_t sent = send(data_upload_socket, &multi_response, sizeof(multi_response), 0);
    //发送数据给上位机应答帧
    if (sent != sizeof(multi_response)) {
        perror("Failed to send buffer query response");
        close(data_upload_socket);
        data_upload_socket = -1;
    } else {
         printf("通道 | 正常数据缓冲区 | 秒脉冲数据缓冲区 | 单机模拟器数据缓冲区\n");
        printf("------------------------------------------------------------\n");
        for (uint8_t ch = 0; ch < 6; ch++) {
            printf("%4d | %14u | %16u | %20u\n",
                   multi_response.channels[ch].channel,
                   multi_response.channels[ch].normal_buf_remaining,
                   multi_response.channels[ch].pulse_buf_remaining,
                   multi_response.channels[ch].sim_buf_remaining);
        }
    }
    pthread_mutex_unlock(&data_upload_mutex);
}
static void send_start_stop_response(uint8_t status) {
    // 提取公共的响应发送逻辑为单独函数
    CAN_Send_Response response;
    memset(&response, 0, sizeof(response));
    uint32_t req_sync_length = reverse_4bytes(sizeof(response));
    response.CAN_DATA_RE_HEAD = reverse_4bytes(ACK_SYNC_WORD);
    response.CAN_DATA_RE_LENGTH = req_sync_length;
    response.CAN_DATA_RE_COUNT = data_counter++;  // 注意：多线程环境下需要加锁保护
    response.CAN_DATA_RE_COMMOND =htons(0x0103);
    response.CAN_DATA_RE_STATUS = status;
    
    // 先设置所有字段再计算校验和（原代码顺序有误，会导致校验和不准确）
    response.CAN_DATA_RE_Checksum = calculate_xor_checksum(
        (const uint8_t*)&response, 
        sizeof(response) - sizeof(response.CAN_DATA_RE_Checksum)
    );
    
    send_can_send_response(&response);
}

static void send_broadcast_config_response(uint8_t status) {
    CAN_Send_Response response;
    memset(&response, 0, sizeof(response));

    response.CAN_DATA_RE_HEAD = reverse_4bytes(ACK_SYNC_WORD);
    response.CAN_DATA_RE_LENGTH = reverse_4bytes(sizeof(response));
    pthread_mutex_lock(&data_counter_mutex);
    data_counter++;
    response.CAN_DATA_RE_COUNT = data_counter;
    pthread_mutex_unlock(&data_counter_mutex);
    response.CAN_DATA_RE_COMMOND = htons(CMD_CAN_BROADCAST_CONFIG);
    response.CAN_DATA_RE_STATUS = status;
    response.CAN_DATA_RE_Checksum = calculate_xor_checksum(
        (const uint8_t *)&response,
        sizeof(response) - sizeof(response.CAN_DATA_RE_Checksum)
    );

    send_can_send_response(&response);
}

static int validate_broadcast_mask(uint32_t mask) {
    return ((mask & ~BCAST_CAN_MASK) == 0);
}

static void reset_broadcast_buffers(void) {
    for (uint8_t ch = 0; ch < AXICAN_MAX; ch++) {
        ringbuffer_32ch_reset(&g_bcast_group_buf_set, ch, AXICAN_MAX);

        pthread_mutex_lock(&g_bcast_wake[ch].mutex);
        g_bcast_wake[ch].pending = 0;
        pthread_mutex_unlock(&g_bcast_wake[ch].mutex);
    }
}

static void handle_broadcast_config(int sock, ReqPacket *req) {
    (void)sock;

    if (!req || !req->data) {
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, EINVAL, "broadcast config request invalid");
        send_broadcast_config_response(1);
        return;
    }

    BroadcastConfigParam *param = (BroadcastConfigParam *)req->data;
    if (param->head != PC_STATUS_HEAD || param->direction != 0 || param->length != sizeof(BroadcastConfigParam)) {
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, EINVAL, "broadcast config header invalid");
        send_broadcast_config_response(1);
        return;
    }

    uint32_t expect_xor = calculate_xor_checksum((uint8_t *)param, sizeof(BroadcastConfigParam) - sizeof(param->xor_checksum));
    if (param->xor_checksum != expect_xor) {
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, EINVAL, "broadcast config checksum invalid");
        send_broadcast_config_response(1);
        return;
    }

    if (param->enable != 0 && param->enable != BCAST_ENABLE) {
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, EINVAL, "broadcast enable invalid");
        send_broadcast_config_response(1);
        return;
    }

    if (param->enable == BCAST_ENABLE &&
        (param->b_mode < BCAST_MODE_SIMULTANEOUS || param->b_mode > BCAST_MODE_AB_ALTERNATE)) {
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, EINVAL, "broadcast mode invalid");
        send_broadcast_config_response(1);
        return;
    }

    if (!validate_broadcast_mask(param->active_mask) ||
        !validate_broadcast_mask(param->group_a_mask) ||
        !validate_broadcast_mask(param->group_b_mask) ||
        param->start_channel >= AXICAN_MAX ||
        param->start_group > 1) {
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, EINVAL, "broadcast mask invalid");
        send_broadcast_config_response(1);
        return;
    }

    pthread_mutex_lock(&bcast_cfg_mutex);
    g_bcast_cfg.enable = param->enable;
    g_bcast_cfg.mode = param->b_mode;
    g_bcast_cfg.start_group = param->start_group;
    g_bcast_cfg.current_group = param->start_group;
    g_bcast_cfg.start_channel = param->start_channel;
    g_bcast_cfg.poll_next_channel = param->start_channel;
    g_bcast_cfg.active_mask = param->active_mask & BCAST_CAN_MASK;
    g_bcast_cfg.group_a_mask = param->group_a_mask & BCAST_CAN_MASK;
    g_bcast_cfg.group_b_mask = param->group_b_mask & BCAST_CAN_MASK;
    g_bcast_cfg.config_count = param->config_count;
    pthread_mutex_unlock(&bcast_cfg_mutex);

    reset_broadcast_buffers();

    printf("[BCAST] config saved: enable=%u mode=%u active=0x%X A=0x%X B=0x%X start_ch=%u start_group=%u\n",
           param->enable, param->b_mode, param->active_mask, param->group_a_mask,
           param->group_b_mask, param->start_channel, param->start_group);
    send_broadcast_config_response(0);
}

// 处理CAN启动/停止命令
static void handle_can_start_stop(int sock, ReqPacket *req) {
    CanStartStopParam *param = (CanStartStopParam *)req->data;
    
    // 校验CAN ID有效性
    printf("##############CAN  ID 0x%08x\n\r",param->can_id);
    if (param->can_id >= AXICAN_MAX) {
        const char *err_msg = "Invalid CAN ID for start/stop";
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, errno, err_msg);
        // 无效ID时也应返回错误响应
        send_start_stop_response(1);  // 1表示错误状态
        return;
    }
    
    uint8_t status = 1;  // 默认错误状态
    printf("CAN RECEIVE STATE CAN ID:%d status: 0x%8X ",param->can_id,param->state);
    uint32_t host_data = ntohl(param->state);
    if(host_data == OPEN_CAN_DEVICE) {
        status = 0;  // 成功状态
        can_configs[param->can_id].state = CAN_STATE_OPENED;
        printf("CAN STATUS OPEN\n\r");
        send_start_stop_response(status);
    }
    else if(host_data == CLOSE_CAN_DEVICE) {
        status = 0;  // 成功状态
        can_configs[param->can_id].state = CAN_STATE_CLOSED;
        printf("CAN STATUS CLOSE\n\r");
        send_start_stop_response(status);
    }
    else {
        // 错误信息修正：原信息与当前操作不匹配
        const char *err_msg = "Invalid CAN state (expected open/close)";
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, errno, err_msg);
        send_start_stop_response(status);  // 发送错误状态
    }
}



// 数据缓存函数，根据传进来的类型存储到相应的缓冲区
int buffer_write_data(uint8_t ch, uint8_t type, DataType data_type, const void *data_ptr) {
    // 1. 参数合法性检查
    if (ch >= AXICAN_MAX || type >= 3 || data_ptr == NULL) {
        printf("buffer_write_data: Invalid param (ch=%d, type=%d, data_ptr=%p)\n", 
               ch, type, data_ptr);
        return -1;
    }

    CanBufferState *buf = &can_buffers[ch][type];
    // 2. 检查缓冲区是否溢出
    if (buf->frame_count >= buf->max_frames) {
        buf->overflow_flag = 1;  // 标记溢出
        printf("buffer_write_data: Buffer overflow (ch=%d, type=%d, max_frames=%d)\n", 
               ch, type, buf->max_frames);
        return -2;
    }

    // 3. 根据数据类型，复制数据到通用帧
    GenericDataFrame *target_frame = &buf->frames[buf->frame_count];
    target_frame->data_type = data_type;  // 先标记数据类型（关键：后续读取依赖此标识）

    switch (data_type) {
        case DATA_TYPE_CAN:
            // 复制CAN数据（PC_ARM_DATA_DATA）到联合体
            memcpy(&target_frame->data.can_msg, data_ptr, sizeof(PC_ARM_DATA_DATA));
            // 更新已用字节数（精确计算：类型标识大小 + 实际数据大小）
            buf->used_size += sizeof(DataType) + sizeof(PC_ARM_DATA_DATA);
            break;
        
        case DATA_TYPE_SECOND_PULSE:
            // 复制秒脉冲数据到联合体
            memcpy(&target_frame->data.pulse_data, data_ptr, sizeof(SecondPulseData));
            buf->used_size += sizeof(DataType) + sizeof(SecondPulseData);
            break;
        
        case DATA_TYPE_SIMULATOR:
            // 复制模拟模式数据到联合体（注意：含512字节数组，需完整复制）
            memcpy(&target_frame->data.sim_data, data_ptr, sizeof(PC_ARM_SIMULATOR_DATA));
            buf->used_size += sizeof(DataType) + sizeof(PC_ARM_SIMULATOR_DATA);
            break;
        
        default:
            printf("buffer_write_data: Unsupported data_type=%d (ch=%d, type=%d)\n", 
                   data_type, ch, type);
            return -3;
    }

    // 4. 更新缓冲区状态（帧数+1，确保used_size不超过total_size）
    buf->frame_count++;
    if (buf->used_size > buf->total_size) {
        buf->used_size = buf->total_size;  // 防止统计错误（理论上不会触发）
    }

    return 0;  // 写入成功

}

// 解析请求包
ParseStatus parse_request_packet(const uint8_t *buffer, uint32_t buf_len, ReqPacket **packet) {
    if (buf_len < sizeof(ReqPacket)) {
        printf("Parse error: buffer too short (min %zu bytes)\n", sizeof(ReqPacket));
        return PARSE_ERR_LEN;
    }
    
    ReqPacket *req = (ReqPacket *)buffer;

    uint32_t req_sync_head = reverse_4bytes(req->sync_word);
    if (req_sync_head != REQ_SYNC_WORD) {
        printf("Parse error: invalid sync word (0x%08X, expected 0x%08X)\n",
               req->sync_word, REQ_SYNC_WORD);
        return PARSE_ERR_SYNC;
    }
    uint32_t checksum_range_len = sizeof(req->sync_word) + sizeof(req->data_len) + 
                                 sizeof(req->counter) + sizeof(req->cmd_type);
    uint32_t expected_checksum = calculate_xor_checksum(buffer, checksum_range_len);
    if (req->checksum != expected_checksum) {
        printf("Parse error: checksum mismatch (receiver 0x%08X, expected 0x%08X)\n",
               req->checksum, expected_checksum);
        return PARSE_ERR_CHECKSUM;
    }
    
    *packet = req;
    return PARSE_SUCCESS;
}

// 时间差计算函数
unsigned long diff_timespec(struct timespec *end_tm, struct timespec *start_tm) {
    struct timespec r;
    unsigned long ret = 0;
    
    r.tv_sec = end_tm->tv_sec - start_tm->tv_sec;
    if (end_tm->tv_nsec < start_tm->tv_nsec) {
        r.tv_nsec = end_tm->tv_nsec + 1000000000L - start_tm->tv_nsec;
        r.tv_sec--;
    } else {
        r.tv_nsec = end_tm->tv_nsec - start_tm->tv_nsec;
    }
    
    ret = r.tv_nsec;
    ret += (unsigned long)r.tv_sec * 1000000000L;
    
    return ret;
}
int axican_read_data_t(int fd, int flags, int id, struct axican_frame *tecv_frame, 
                     RingBuffer32ChSet *buf_set, uint8_t max_channel) {
    // 参数合法性检查（新增缓冲区相关校验）
    if (fd < 0 || !tecv_frame || !buf_set) {
        printf("[%s] 错误: 无效参数 (fd=%d, recv_frame=%p, buf_set=%p)\n", 
               __func__, fd, (void*)tecv_frame, (void*)buf_set);
        return -1;
    }
    if (id < 0 || id >= max_channel) {
        printf("[%s] 错误: 通道号无效 (id=%d, 最大通道数=%d)\n", 
               __func__, id, max_channel);
        return -1;
    }

    int i, rc, frame_count;
    frame_count = axican_get_frame_count(fd);
    if (frame_count <= 0) {
        return 0;
    }
    if(frame_count>=1024)
    {
        printf("frame_count>=1024,frame_count%d",frame_count);
    }
    uint32_t put_result;

    // 循环读取帧并写入环形缓冲区
    for (i = 0; i < frame_count; i++) {
        memset(tecv_frame, 0x0, sizeof(struct axican_frame));
        
        // 调用axican_read读取数据
        rc = axican_read(fd, flags, (unsigned char *)tecv_frame, sizeof(struct axican_frame));
        
        if (rc != sizeof(struct axican_frame)) {
            printf("[%s] can%d read count %d data error (实际读取:%d字节)\n", 
                   __func__, id, i, rc);
            return i;  // 返回已成功处理的帧数
        }

        // 将数据写入环形缓冲区（指定通道为id）
        put_result = ringbuffer_32ch_put(&g_poll_buf_set, id, tecv_frame, max_channel);
        if (put_result == 0) {
            printf("[CAN%d] 警告: 缓冲区满，监控线程，第%d帧写入失败\n", id, i);
            // 可根据需求选择继续或退出
            // break;
        }
    }

    return frame_count;
}

// CAN数据读取函数
int axican_read_data(int fd, int flags, int id, struct axican_frame *recv_frame, 
                     RingBuffer32ChSet *buf_set, uint8_t max_channel) {
    // 参数合法性检查（新增缓冲区相关校验）
    if (fd < 0 || !recv_frame || !buf_set) {
        printf("[%s] 错误: 无效参数 (fd=%d, recv_frame=%p, buf_set=%p)\n", 
               __func__, fd, (void*)recv_frame, (void*)buf_set);
        return -1;
    }
    if (id < 0 || id >= max_channel) {
        printf("[%s] 错误: 通道号无效 (id=%d, 最大通道数=%d)\n", 
               __func__, id, max_channel);
        return -1;
    }

    int i, rc, frame_count;
    frame_count = axican_get_frame_count(fd);
    if (frame_count <= 0) {
        return 0;
    }
    uint32_t put_result;

    // 循环读取帧并写入环形缓冲区
    for (i = 0; i < frame_count; i++) {
        memset(recv_frame, 0x0, sizeof(struct axican_frame));
        
        // 调用axican_read读取数据
        rc = axican_read(fd, flags, (unsigned char *)recv_frame, sizeof(struct axican_frame));
        
        if (rc != sizeof(struct axican_frame)) {
            printf("[%s] can%d read count %d data error (实际读取:%d字节)\n", 
                   __func__, id, i, rc);
            return i;  // 返回已成功处理的帧数
        }

        // 将数据写入环形缓冲区（指定通道为id）
        put_result = ringbuffer_32ch_put(&g_can_buf_set, id, recv_frame, max_channel);
        //printf("[CAN%d接收线程] #压入缓冲区帧数：%d\n", id, i+1);
        if (put_result == 0) {
            printf("[CAN%d] 警告: 缓冲区满，第%d帧写入失败\n", id, i);
            // 可根据需求选择继续或退出
            // break;
        }
    }

    return frame_count;
}
//单机模拟收到数据返回给上位机
void send_can_tx_status(uint8_t ch_id,uint32_t data_array[],uint8_t status,uint8_t type)
{

    CAN_ARM_PC_DATA status_frame;
    status_frame.head=0x499602D2;
    status_frame.direction=0x01;
    status_frame.type= type;
    status_frame.length=0x30;
    status_frame.ch_id = ch_id;
    status_frame.total_count = (data_counter == 0xFFFFFFFF) ? 0 : data_counter + 1;
    channel_frame_count[status_frame.ch_id] = (channel_frame_count[status_frame.ch_id] == 0xFFFFFFFF) ? 0 : channel_frame_count[status_frame.ch_id] + 1;
    status_frame.ch_count = channel_frame_count[status_frame.ch_id];
    status_frame.idr=data_array[0];
    status_frame.drcr=data_array[1];//(frame->can_id >> 28) & 0x07;
    status_frame.dw1r=data_array[3];
    status_frame.dw2r=data_array[4];
    status_frame.status= status;
    status_frame.reserved1=0;
    status_frame.reserved2 = 0;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    status_frame.timestamp=(uint64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
    status_frame.xor_checksum=calculate_xor_checksum((uint8_t *)&status_frame, sizeof(CAN_ARM_PC_DATA) - 4);
    //通过上传数据端口发送给9010端口
    pthread_mutex_lock(&data_upload_mutex);
    if (data_upload_socket == -1) {
        pthread_mutex_unlock(&data_upload_mutex);
        printf("No data client connected, cannot send transmit response\n");
        return;
    }
    ssize_t sent = send(data_upload_socket, &status_frame, sizeof(status_frame), 0);
    if (sent != sizeof(status_frame)) {
        perror("Failed to send transmit response");
        close(data_upload_socket);
        data_upload_socket = -1;
    } else {
        printf("Sent transmit response for CAN %d: status=%d\n",
               ch_id, status);
    }
}
//从缓冲区提取数据
int can_buffer_extract(int ch, DataType data_type, GenericDataFrame *frame) {
    // 1. 参数合法性检查
    if (ch >= AXICAN_MAX) 
    {
        printf("Invalid channel %d or type %d\n", ch, data_type);
        return -1;
    }
    if (frame == NULL) {
        printf("frame pointer is NULL\n");
        return -1;
    }
    
    // 2. 获取对应类型的缓冲区
    CanBufferState *buf = &can_buffers[ch][data_type];
    if (buf->frames == NULL) {
        printf("Buffer ch%d type%d not initialized\n", ch, data_type);
        return -3;
    }
    
    // 3. 检查缓冲区是否为空（调用方已持有锁，无需重复加锁）
    if (buf->frame_count == 0) {
        return -2;  // 缓冲区空
    }
    
    // 4. 提取首帧数据（FIFO）
    memcpy(frame, &buf->frames[0], sizeof(GenericDataFrame));
    
    // 5. 移动后续帧覆盖已提取帧
    size_t move_count = buf->frame_count - 1;
    if (move_count > 0) {
        memmove(&buf->frames[0], &buf->frames[1], 
               move_count * sizeof(GenericDataFrame));
    }
    
    // 6. 更新缓冲区状态
    buf->frame_count--;
    buf->used_size = buf->frame_count * sizeof(GenericDataFrame);
    buf->overflow_flag = 0;  // 提取后溢出标志重置
    
    return 0;  // 成功
}
static void print_can_frame_detail(const struct axican_frame *frame) {
    if (frame == NULL) return;  // 防空指针

    // 1. 打印基础信息（ID、DLC、时间戳）
    printf("=== Received CAN Frame Detail ===\n");
    printf("CAN ID:    0x%08X (decimal: %u)\n", frame->can_id, frame->can_id);
    printf("DLC:       0x%08x bytes (0~8 legal)\n", frame->can_dlc);
    printf("Timestamp: %u ms (since start)\n", frame->timestamp);  // 假设timestamp单位是ms

    // 2. 打印数据内容（按字节十六进制+十进制显示，便于核对）
    printf("Data:      ");
    for (uint8_t i = 0; i < frame->can_dlc; i++) {
        printf("0x%02X (%3u) \n", frame->data[i], frame->data[i]);
    }
    // 若DLC为0（空数据帧），提示无数据
    if (frame->can_dlc == 0) {
        printf("(empty frame)");
    }
}
//初始化一个临时缓冲区
// static void init_packet_buffer(int can_id) {
//     pthread_mutex_lock(&packet_mutex);
//     packet_buffers[can_id].can_id = can_id;
//     packet_buffers[can_id].total_packets = 0;
//     packet_buffers[can_id].received_packets = 0;
//     packet_buffers[can_id].last_receive_time = time(NULL);
//     packet_buffers[can_id].is_active = false;
//     memset(packet_buffers[can_id].frames, 0, sizeof(GenericDataFrame) * MAX_PACKETS);
//     pthread_mutex_unlock(&packet_mutex);
// }
//比较是否连续
// 检查分包是否连续完整
// static bool check_packets_complete(PacketBuffer *buf) {
//     if (buf->received_packets != buf->total_packets) {
//         return false;
//     }
    
//     // 检查序列连续性
//     for (int i = 0; i < buf->total_packets; i++) {
//         // 假设序列包数存储在frame的sequence字段
//         if (buf->frames[i].data.can_msg.packets_no!= i) {
//             return false;
//         }
//     }
//     return true;
// }
//CAN正常接收数据进行处理(单机模式)
static void handle_along_mode(int can_id, struct axican *axican_info, 
                             CanFourByteStruct fourByteFrames[], 
                             size_t fourByteFramesSize) {  // 新增参数：数组实际大小
    // 1. 更新接收计数器并通知等待线程
    pthread_mutex_lock(&along_counter_mutex);
    along_more_int_couter++;
    printf("[CAN%d(ALONG)] 接收计数器更新：%d\n", can_id, along_more_int_couter);
    pthread_cond_signal(&along_counter_cond);
    pthread_mutex_unlock(&along_counter_mutex);

    // 2. 获取模拟数据缓冲区
    CanBufferState *sim_buf = &can_buffers[can_id][ALONG_MODE];
    if (sim_buf == NULL || sim_buf->frames == NULL) {
        printf("[CAN%d(ALONG)] 错误：模拟缓冲区未初始化\n", can_id);
        return;
    }
    if (sim_buf->frame_count == 0) {
        printf("[CAN%d(ALONG)] 提示：模拟缓冲区无数据\n", can_id);
        return;
    }

    // 3. 静态互斥锁初始化
    static pthread_mutex_t buffer_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&buffer_mutex);

    // 4. 遍历模拟数据帧
    for (size_t i = 0; i < sim_buf->frame_count; i++) {
        GenericDataFrame *sim_frame = &sim_buf->frames[i];
        if (sim_frame->data_type != DATA_TYPE_SIMULATOR) {
            printf("[CAN%d(ALONG)] 警告：跳过无效帧类型（索引：%zu,类型：%d:)\n", 
                   can_id, i, sim_frame->data_type);
            continue;
        }

        PC_ARM_SIMULATOR_DATA *sim_data = &sim_frame->data.sim_data;
        // 修复memcmp参数：将uint32_t转换为指针（&sim_data->data_type）
        const bool is_match = (sim_data->can_id == axican_info->rx_frame.can_id) && 
                             (memcmp(&sim_data->data_type, axican_info->rx_frame.data, 4) == 0);
        if (!is_match) {
            printf("[CAN%d(ALONG)] 提示：帧不匹配(模拟ID:0x%x,接收ID:0x%x)\n", 
                   can_id, sim_data->can_id, axican_info->rx_frame.can_id);
            continue;
        }

        // 5. 上传接收数据给上位机
        uint32_t can_frame_data[4] = {0};
        memcpy(can_frame_data, axican_info->rx_frame.data, sizeof(can_frame_data));
        send_can_tx_status(can_id, can_frame_data, 1, 0x82);
        printf("[CAN%d(ALONG)] 已上传接收数据(CAN ID:0x%x)\n", can_id, sim_data->can_id);

        // 6. 等待接收足够帧数
        const int expected_frames = sim_data->rcv_len / 8;
        pthread_mutex_lock(&along_counter_mutex);
        while (along_more_int_couter < expected_frames) {
            printf("[CAN%d(ALONG)] 等待足够帧数（当前：%d:期望:=%d)\n", 
                   can_id, along_more_int_couter, expected_frames);
            pthread_cond_wait(&along_counter_cond, &along_counter_mutex);
        }
        along_more_int_couter = 0;
        pthread_mutex_unlock(&along_counter_mutex);

        // 7. 组帧并发送到CAN
        if (sim_data->send_data == NULL || sim_data->send_len == 0) {
            printf("[CAN%d(ALONG)] 错误：模拟发送数据为空\n", can_id);
            continue;
        }
        const size_t send_loop_cnt = sim_data->send_len / CAN_FIFO_TWO_FRAME_SIZE;
        for (size_t i_frame = 0; i_frame < send_loop_cnt; i_frame++) {
            const size_t frame_offset = i_frame * CAN_FIFO_TWO_FRAME_SIZE;
            if (frame_offset + CAN_FIFO_TWO_FRAME_SIZE > sim_data->send_len) {
                printf("[CAN%d(ALONG)] 警告：发送数据越界，跳过当前帧\n", can_id);
                break;
            }
            uint8_t *current_frame = sim_data->send_data + frame_offset;

            // 拆分4字节子帧
            for (size_t j = 0; j < CAN_FIFO_THR_FRAME_SIZE; j++) {
                const size_t subframe_offset = j * 4;
                uint8_t *current_subframe = current_frame + subframe_offset;
                printf("[CAN%d(ALONG)] 组帧地址：%p(子帧索引：%zu)\n", 
                       can_id, (void*)current_subframe, j);
                memcpy(fourByteFrames[j].bytes, current_subframe, 4);
            }

            // 修复sizeof警告：使用传入的数组实际大小
            if (axican_test[can_id].f_wr.fd < 0) {
                printf("[CAN%d(ALONG)] 错误：发送文件描述符无效(fd:%d)\n", 
                       can_id, axican_test[can_id].f_wr.fd);
                continue;
            }
            const ssize_t write_ret = write(axican_test[can_id].f_wr.fd, 
                                           fourByteFrames, 
                                           fourByteFramesSize);  // 使用实际大小
            if (write_ret != fourByteFramesSize) {  // 比较实际大小
                printf("[CAN%d(ALONG)] 错误:发送CAN失败(写入:%zd字节,期望：%zu字节,err:%d)\n", 
                       can_id, write_ret, fourByteFramesSize, errno);  // 打印实际大小
            } else {
                printf("[CAN%d(ALONG)] 发送CAN成功(帧数：%zu)\n", 
                       can_id, CAN_FIFO_THR_FRAME_SIZE);
                send_can_tx_status(can_id, (uint32_t*)fourByteFrames[0].bytes, 1, 0x82);
            }
        }
    }

    pthread_mutex_unlock(&buffer_mutex);
}
//CAN正常接收数据进行处理(正常模式)
static void handle_normal_mode(int can_id, const struct axican_frame* frame) {
    // 1. 初始化响应结构体（避免脏数据）
    CAN_ARM_PC_DATA can_resp = {0};
    can_resp.PC_ARM_DATE_HEAD = PC_ARM_STATUS_WORD;
    can_resp.PC_ARM_DATE_LENGTH = reverse_4bytes(sizeof(CAN_ARM_PC_DATA)-20);
    can_resp.PC_ARM_DATE_COUNTER =0;
    can_resp.PC_ARM_DATE_COMMAND = 0;
    can_resp.PC_ARM_CHIND =reverse_4bytes( can_id);

    can_resp.head = PC_STATUS_HEAD;
    can_resp.direction = 0x1;
    can_resp.type = 0x81;
    can_resp.length = 0x30;  // 固定长度（需确认是否与上位机协议一致）
    can_resp.ch_id = can_id;
    can_resp.total_count = (data_counter == 0xFFFFFFFF) ? 0 : data_counter + 1; 
    data_counter = can_resp.total_count;  // 更新全局计数器
    
    // 从struct axican_frame中获取CAN ID和数据长度
    can_resp.idr = frame->can_id;
    can_resp.drcr = frame->can_dlc;

    // 2. 更新通道帧计数（处理溢出）
    if (channel_frame_count[can_id] == 0xFFFFFFFF) {
        channel_frame_count[can_id] = 0;
    } else {
        channel_frame_count[can_id]++;
    }
    can_resp.ch_count = channel_frame_count[can_id];
    can_resp.idr=frame->can_id;
    can_resp.drcr= frame->can_dlc>>28;

    // 3. 填充数据（从struct axican_frame的data字段获取）
    can_resp.dw1r = frame->data[0];  // 协议：仅取data[0]作为dw1r
    can_resp.dw2r = frame->data[1];  

    // 4. 使用传入的时间戳
    can_resp.timestamp = frame->timestamp;
    
    // 计算校验和
    const size_t checksum_len = sizeof(CAN_ARM_PC_DATA) - sizeof(can_resp.xor_checksum);
    can_resp.xor_checksum = calculate_xor_checksum((uint8_t *)&can_resp, checksum_len);
   // printf("[CAN%d(NORMAL)] 构建响应:CH=%d, CAN_ID=0x%x, 计数=%d, 校验和=0x%x\n", can_id, can_resp.ch_id, can_resp.idr, can_resp.ch_count, can_resp.xor_checksum);

    // 5. 发送给上位机（加锁保护socket资源）
    pthread_mutex_lock(&data_upload_mutex);
    if (data_upload_socket == -1) {
        pthread_mutex_unlock(&data_upload_mutex);
        printf("[CAN%d(NORMAL)] 警告：无上位机连接（端口%d),跳过发送\n", can_id, TCP_PORT2);
        return;
    }

    // 发送响应（检查send返回值，处理部分发送场景）
    const ssize_t sent_len = send(data_upload_socket, &can_resp, sizeof(can_resp), 0);
    if (sent_len < 0) {
        fprintf(stderr, "[CAN%d(NORMAL)] 错误：发送上位机失败: %s\n", can_id, strerror(errno));
        close(data_upload_socket);
        data_upload_socket = -1;  // 标记socket失效
    } else if (sent_len != sizeof(can_resp)) {
        printf("[CAN%d(NORMAL)] 错误：发送长度不完整（发送：%zd字节,期望：%zu字节)\n", 
               can_id, sent_len, sizeof(can_resp));
        close(data_upload_socket);
        data_upload_socket = -1;
    } else {
        //printf("[CAN%d(NORMAL)] 成功发送上位机（端口%d,长度：%zu字节)\n", can_id, TCP_PORT2, sizeof(can_resp));
    }
    pthread_mutex_unlock(&data_upload_mutex);
}

// CAN数据接收线程
static void *axican_data_recv_thread(void* parameter) {
    int rc;
    int poll_rc;                  // poll返回值
    int read_rc;                  // 读数据返回值
    unsigned int poll_timeout = 100;
    struct axican *axican_info = (struct axican *)parameter;
    int can_id = axican_info->id;
    // 假设fourByteFrames的定义和使用在handle_along_mode中有说明

    cpu_set_t cpuset;
    CPU_SET(1, &cpuset);    
    pthread_t tid = pthread_self();
    int rc_c = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
    if (rc_c != 0) {
        printf("[CAN%d] 设置CPU亲和性失败: %d\n", can_id, rc);
    } else {
       // printf("[CAN%d] 线程已绑定到CPU核心: %d\n", can_id, can_id % sysconf(_SC_NPROCESSORS_ONLN));
    }
    
    // 设置线程优先级
    struct sched_param param_a;
    param_a.sched_priority = 60;  // 优先级值(1-99，值越大优先级越高)
    
    // 使用实时调度策略(SCHED_FIFO)
    rc_c = pthread_setschedparam(tid, SCHED_FIFO, &param_a);
    if (rc_c != 0) {
        printf("[CAN%d] 设置线程优先级失败: %d\n", can_id, rc);
    } else {
       // printf("[CAN%d] 线程优先级已设置为: %d\n", can_id, param_a.sched_priority);
    }

    printf("###### %s axican id:%d ######## \n", __func__, axican_info->id);
    // 参数合法性检查（避免空指针崩溃）
    if (axican_info == NULL) {
        printf("[CAN接收线程] 错误,参数axican_info为空,线程退出\n");
        return NULL;
    }

    while (running) {
        // --------------------------
        // 阶段1：等待CAN设备开启
        // --------------------------
        CanState current_state = can_configs[can_id].state;
        while(running && current_state != CAN_STATE_OPENED)
        {
            usleep(100000); 
            current_state = can_configs[can_id].state;
        }
        if (!running) {
            break;
        }
        
        uint8_t current_can_type = can_configs[can_id].can_type;
        poll_rc = axican_poll2(axican_info->f_rd.fd, axican_info->f_rd.flags, poll_timeout);
        
        if (!poll_rc) {
            // 读取CAN数据（axican_read_data返回：成功读取的帧数）
            read_rc = axican_read_data(axican_info->f_rd.fd, axican_info->f_rd.flags, can_id, &axican_info->rx_frame,&g_can_buf_set,AXICAN_CHAN_MAX);
            //printf("[CAN%d接收线程] 读取数据完成，帧数：%d\n", can_id, read_rc);
            // RX日志频率很高，需要排查接收数据时再打开。
            // printf("####axican_info->rx_frame.id:0x%08x\n",axican_info->rx_frame.can_id);
            int sim_ready = ringbuffer_32ch_len(&g_can_buf_set, can_id,6);
            //printf("[CAN%d接收线程] 压入缓冲区帧数：%d\n", can_id, sim_ready);

            // 错误处理：读取失败（read_rc=0）
            if (read_rc < 0) {
                printf("[CAN%d接收线程] 读取函数错误 flag:%d\n", can_id, axican_info->f_rd.flags);
                usleep(10000); // 短暂休眠避免忙等
            }



        } else if (poll_rc < 0) {
            //printf("[CAN%d接收线程] 错误:poll接收通道失败,err=%d(%s)\n", can_id, errno, strerror(errno));
        }

        // // 处理发送通道
        // rc = axican_poll2(axican_info->f_wr.fd, axican_info->f_wr.flags, poll_timeout);
        // if (!rc) {
        //     rc = axican_read_data(axican_info->f_wr.fd, axican_info->f_wr.flags, axican_info->id, &axican_info->tx_frame);
        //     if (!rc) {
        //         printf("axican%d read data error flag:%d \n", axican_info->id, axican_info->f_wr.flags);
        //     }
        // }
    }

    return NULL;
}

// CAN数据发送线程
static void send_single_frame(struct axican *axican_info, PC_ARM_DATA_DATA *curr_frame) {
    struct axican_frame send_frame;
    int rc;
    
    memset(&send_frame, 0, sizeof(send_frame));
    send_frame.can_id = curr_frame->idr;
    // send_frame.can_dlc = curr_frame->drcr << 28;
    send_frame.can_dlc = curr_frame->drcr;
    send_frame.data[0] = reverse_4bytes(curr_frame->dw1r);
    send_frame.data[1] = reverse_4bytes(curr_frame->dw2r);
    // printf("##############send_frame.can_dlc :0x%08x,send_frame:0x%08x\n\r",curr_frame->dw1r ,send_frame.data[0]);
    // printf("##############send_frame.can_dlc :0x%08x,send_frame:0x%08x\n\r",curr_frame->dw2r,send_frame.data[1] );
    //printf("##############send_frame.can_dlc :0x%08x,send_frame:0x%08x\n\r",send_frame.can_dlc );
    // 设置时间戳
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
        send_frame.timestamp = (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    } else {
        send_frame.timestamp = 0;
        printf("[CAN%d发送线程] 警告：获取时间戳失败\n", axican_info->id);
    }

    axican_info->w_count +=1;
    
    // 发送数据
    // 普通数据是高频发送路径，禁止逐帧打印，避免串口/SSH 日志阻塞发送线程。
    // printf("[CAN%d NORMAL TX] id=0x%08X dlc=0x%08X data0=0x%08X data1=0x%08X ts=%lld\n",
    //        axican_info->id, send_frame.can_id, send_frame.can_dlc,
    //        send_frame.data[0], send_frame.data[1], send_frame.timestamp);
    pthread_mutex_lock(&can_write_mutex[axican_info->id]);
    rc = write(axican_info->f_wr.fd, &send_frame, sizeof(send_frame));
    pthread_mutex_unlock(&can_write_mutex[axican_info->id]);
    if (rc != sizeof(send_frame)) {
        send_can_transmit_response(axican_info->id,0x80, 0, &send_frame);
        printf("[CAN%d发送线程] 错误：发送失败 - 写入 %d/%zu 字节\n",
               axican_info->id, rc, sizeof(send_frame));
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, errno, "CAN data send failed");
    } else {
       // send_can_transmit_response(axican_info->id, 1, &send_frame);
    }
}
bool ringbuffer_32ch_target_is_inited(RingBuffer32ChSet *set, uint8_t target_ch, uint32_t expected_elem_size) {
    // 1. 基础参数校验
    if (set == NULL) {
        printf("[RingBuffer] Check failed: Buffer set is NULL\n");
        return false;
    }
    
    if (target_ch >= 32) {
        printf("[RingBuffer] Check failed: Invalid target channel %u (must be 0-31)\n", target_ch);
        return false;
    }

    // 2. 校验元素大小匹配（确保缓冲区存储正确的数据类型）
    if (set->elem_size != expected_elem_size) {
        printf("[RingBuffer] Check failed: Elem size mismatch for channel %u (exp: %u, act: %u)\n",
               target_ch, expected_elem_size, set->elem_size);
        return false;
    }

    // 3. 校验目标通道的缓冲区是否已创建且有效
    if (set->channels[target_ch] == NULL) {
        printf("[RingBuffer] Check failed: Channel %u buffer not created\n", target_ch);
        return false;
    }

    // 4. 校验目标通道缓冲区的实际数据存储区是否分配成功
    if (set->channels[target_ch]->data == NULL) {
        printf("[RingBuffer] Check failed: Channel %u data buffer is NULL\n", target_ch);
        return false;
    }

    // 5. 校验目标通道缓冲区的基础参数合理性
    ring_buffer_t *target_rb = set->channels[target_ch];
    if (target_rb->esize != expected_elem_size) {
        printf("[RingBuffer] Check failed: Channel %u elem size mismatch (exp: %u, act: %u)\n",
               target_ch, expected_elem_size, target_rb->esize);
        return false;
    }

    if (target_rb->mask + 1 == 0) {  // mask = 容量 - 1，容量为0时 mask = -1（无符号下为最大值）
        printf("[RingBuffer] Check failed: Channel %u has zero capacity\n", target_ch);
        return false;
    }

    // 所有检查通过
    // printf("[RingBuffer] Check passed: Channel %u buffer inited (elem_size: %u, capacity: %u)\n",
    //        target_ch, expected_elem_size, target_rb->mask + 1);
    return true;
}

static void *axican_data_send_thread(void* parameter) {
    int rc;
    struct axican_frame send_frame;
    struct axican *axican_info = (struct axican *)parameter;
    uint8_t can_id = axican_info->id;
    uint32_t elem_size = sizeof(PC_ARM_DATA_DATA);
    PC_ARM_DATA_DATA curr_frame;  // 单帧数据缓冲区

    // 普通发送优先级低于广播发送和 TX 监控。
    struct sched_param normal_param;
    memset(&normal_param, 0, sizeof(normal_param));
    normal_param.sched_priority = 80;
    int sched_rc = pthread_setschedparam(pthread_self(), SCHED_FIFO, &normal_param);
    if (sched_rc != 0) {
        fprintf(stderr, "[CAN%d NORMAL TX] 设置SCHED_FIFO优先级80失败: %s\n",
                can_id, strerror(sched_rc));
    }


    // 检查缓冲区初始化状态
    bool is_ready = ringbuffer_32ch_target_is_inited(
        &g_data_buf_set, can_id, elem_size
    );
    if (!is_ready) {
        printf("[CAN%d发送线程] 错误：缓冲区未初始化，线程退出\n", can_id);
        return NULL;
    }

    while (running) {
        
        // 等待CAN通道就绪且有数据
        while (running && (can_configs[can_id].state != CAN_STATE_OPENED || 
                          ringbuffer_32ch_len(&g_data_buf_set, can_id,32) == 0)) {
            usleep(1000);
            if((axican_info->id!=4) && (axican_info->id!=5))
            {
                //printf("[can id %d] can w_count :0x%08x\n\r",axican_info->id,axican_info->w_count);
            }
            
        }
        // printf("CAN通道有数据发送\n\r");

        if (!running) break;

        // 每次只提取1帧数据
        uint32_t get_ret = ringbuffer_32ch_get(
            &g_data_buf_set, can_id, &curr_frame,32
        );
        
        // 提取失败时等待并重试
        if (get_ret != 1) {
            printf("[CAN%d发送线程] 提取数据失败\n", can_id);
            usleep(1000);
            continue;
        }

        // 校验同步字
        if (curr_frame.head != PC_STATUS_HEAD) {
            printf("[CAN%d发送线程] 无效帧：同步字不匹配（实际:0x%08X,预期:0x%08X)\n",
                   can_id, curr_frame.head, PC_STATUS_HEAD);
            send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, EINVAL, "Invalid sync word");
            continue;
        }

        // 校验通道匹配性
        uint8_t frame_can_id = curr_frame.ch_id;
        if (frame_can_id != can_id) {
            printf("[CAN%d发送线程] 无效帧：通道不匹配（帧通道:%d,线程通道:%d)\n",
                   can_id, frame_can_id, can_id);
            send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, EINVAL, "Channel mismatch");
            continue;
        }

        uint8_t sequence = curr_frame.packets_no;
        uint8_t total_packets = curr_frame.packets_sum;

        // 单包数据直接发送
        if (total_packets == 1) {
            //  int  frame_count = axican_get_frame_count(axican_info->f_rd.fd);
            //  printf("frame_count :%d\n\r",frame_count);
            send_single_frame(axican_info, &curr_frame);
            continue;
        }

        // 多包数据处理（加锁保护）
        if (pthread_mutex_lock(&packet_mutex) != 0) {
            printf("[CAN%d发送线程] 错误：获取数据包互斥锁失败\n", can_id);
            continue;
        }

        PacketBuffer *pkt_buf = &packet_buffers[can_id];
        time_t current_time = time(NULL);

        // 检查超时并重置
        if (pkt_buf->is_active && 
            (current_time - pkt_buf->last_receive_time) * 1000 > PACKET_TIMEOUT) {
            printf("[CAN%d发送线程] 警告：数据包超时（重置缓冲区)\n", can_id);
            memset(pkt_buf, 0, sizeof(PacketBuffer));
        }

        // 处理第一个包（初始化缓冲区）
        if (sequence == 0) {
            if (total_packets <= 0 || total_packets > DEQUEUE_BATCH_SIZE_1) {
                printf("[CAN%d发送线程] 无效帧：总包数非法（总包数:%d)\n",
                       can_id, total_packets);
                pthread_mutex_unlock(&packet_mutex);
                continue;
            }
            memset(pkt_buf, 0, sizeof(PacketBuffer));
            pkt_buf->total_packets = total_packets;
            pkt_buf->is_active = true;
        }

        // 校验数据包状态
        if (!pkt_buf->is_active || sequence >= pkt_buf->total_packets) {
            printf("[CAN%d发送线程] 无效帧：数据包未激活或序号非法（序号:%d,总包数:%d)\n",
                   can_id, sequence, pkt_buf->total_packets);
            pthread_mutex_unlock(&packet_mutex);
            continue;
        }

        // 检查包连续性
        if (sequence != pkt_buf->received_packets) {
            printf("[CAN%d发送线程] 无效帧：数据包不连续（序号:%d,已接收包数:%d）\n",
                   can_id, sequence, pkt_buf->received_packets);
            memset(pkt_buf, 0, sizeof(PacketBuffer)); // 重置不连续的包
            pthread_mutex_unlock(&packet_mutex);
            continue;
        }

        // 存储当前包
        pkt_buf->frames[sequence] = curr_frame;
        pkt_buf->received_packets++;
        pkt_buf->last_receive_time = current_time;

        // 所有包接收完成，逐帧发送
        if (pkt_buf->received_packets == pkt_buf->total_packets) {
            for (uint8_t j = 0; j < pkt_buf->total_packets; j++) {
            //    int  frame_count = axican_get_frame_count(axican_info->f_rd.fd);
            //    printf("frame_count :%d\n\r",frame_count);

                send_single_frame(axican_info, &pkt_buf->frames[j]);
                //usleep(1000);
            }
            memset(pkt_buf, 0, sizeof(PacketBuffer)); // 重置缓冲区
        }

        pthread_mutex_unlock(&packet_mutex); // 确保解锁
    }

    printf("[CAN%d发送线程] 线程退出\n", can_id);
    return NULL;
}

//写入缓存
int can_buffer_insert_batch(uint8_t can_id, DataType data_type, const void *data, size_t count, size_t data_len) {
    if (!data || count == 0 || can_id >= 32) {
        return -EINVAL;
    }

    uint32_t put_count = 0;
    switch (data_type) {
        case DATA_TYPE_CAN:
            put_count = ringbuffer_32ch_put(&g_data_buf_set, can_id, data,32);
            break;
        case DATA_TYPE_SIMULATOR:
            put_count = ringbuffer_32ch_put(&g_simulator_buf_set, can_id, data,32);
            break;
        case DATA_TYPE_SECOND_PULSE:
            put_count = ringbuffer_32ch_put(&g_pulse_buf_set, can_id, data,32);
            break;
        // case DATA_TYPE_DMA:
        //     put_count = ringbuffer_32ch_put(&g_dma_buf_set, can_id, data);
        //     break;
        default:
            return -EINVAL;
    }

    return (put_count == count) ? (int)count : -ENOMEM;
}

static uint32_t next_broadcast_group_id(void) {
    uint32_t group_id;

    pthread_mutex_lock(&bcast_group_id_mutex);
    g_bcast_group_id = (g_bcast_group_id == 0xFFFFFFFFU) ? 1 : g_bcast_group_id + 1;
    group_id = g_bcast_group_id;
    pthread_mutex_unlock(&bcast_group_id_mutex);

    return group_id;
}

static uint32_t get_broadcast_store_mask(void) {
    uint32_t mask = 0;

    pthread_mutex_lock(&bcast_cfg_mutex);
    if (g_bcast_cfg.enable == BCAST_ENABLE) {
        switch (g_bcast_cfg.mode) {
            case BCAST_MODE_SIMULTANEOUS:
            case BCAST_MODE_POLLING:
                mask = g_bcast_cfg.active_mask;
                break;
            case BCAST_MODE_AB_ALTERNATE:
                mask = g_bcast_cfg.group_a_mask | g_bcast_cfg.group_b_mask;
                break;
            default:
                mask = 0;
                break;
        }
    }
    pthread_mutex_unlock(&bcast_cfg_mutex);

    return mask & BCAST_CAN_MASK;
}

static int validate_48byte_can_payload(const PC_ARM_DATA_DATA *frame) {
    if (!frame) {
        return -EINVAL;
    }
    if (frame->head != PC_STATUS_HEAD || frame->length != sizeof(PC_ARM_DATA_DATA)) {
        return -EINVAL;
    }

    uint32_t expect_xor = calculate_xor_checksum(
        (uint8_t *)frame,
        sizeof(PC_ARM_DATA_DATA) - sizeof(frame->xor_checksum)
    );
    if (frame->xor_checksum != expect_xor) {
        return -EINVAL;
    }

    return 0;
}

static void pc_can_payload_to_axican_frame(const PC_ARM_DATA_DATA *src, struct axican_frame *dst) {
    memset(dst, 0, sizeof(*dst));
    dst->can_id = src->idr;
    dst->can_dlc = src->drcr;
    dst->data[0] = reverse_4bytes(src->dw1r);
    dst->data[1] = reverse_4bytes(src->dw2r);
    dst->timestamp = src->timestamp;
}

static int enqueue_normal_can_payload(const PC_ARM_DATA_DATA *frame) {
    uint8_t can_id = frame->ch_id;

    if (can_id >= AXICAN_MAX || !can_configs[can_id].valid) {
        printf("[9012] normal CAN channel invalid: ch=%u\n", can_id);
        return -EINVAL;
    }

    if (ringbuffer_32ch_avail(&g_data_buf_set, can_id, 32) == 0) {
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, -ENOMEM, "normal CAN buffer full");
        return -ENOMEM;
    }

    return (ringbuffer_32ch_put(&g_data_buf_set, can_id, frame, 32) == 1) ? 0 : -ENOMEM;
}

static int enqueue_broadcast_group(uint8_t target_ch, const PC_ARM_DATA_DATA *frames, size_t frame_count) {
    if (!frames || frame_count == 0 || frame_count > BCAST_MAX_FRAMES_PER_GROUP) {
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, EINVAL, "broadcast group size invalid");
        return -EINVAL;
    }

    if (target_ch >= AXICAN_MAX || !can_configs[target_ch].valid) {
        printf("[BCAST] invalid packet channel %u\n", target_ch);
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, EINVAL, "broadcast packet channel invalid");
        return -EINVAL;
    }

    if (ringbuffer_32ch_avail(&g_bcast_group_buf_set, target_ch, AXICAN_MAX) == 0) {
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, -ENOMEM, "broadcast channel buffer full");
        return -ENOMEM;
    }

    BroadcastFrameGroup group;
    memset(&group, 0, sizeof(group));
    group.group_id = next_broadcast_group_id();
    group.frame_count = (uint16_t)frame_count;

    for (size_t i = 0; i < frame_count; i++) {
        pc_can_payload_to_axican_frame(&frames[i], &group.frames[i]);
    }

    return (ringbuffer_32ch_put(&g_bcast_group_buf_set, target_ch, &group, AXICAN_MAX) == 1) ? 0 : -ENOMEM;
}

static int process_9012_can_payload_frames(const uint8_t *payload, size_t payload_len, uint32_t packet_channel) {
    const size_t frame_size = sizeof(PC_ARM_DATA_DATA);
    if (!payload || payload_len == 0 || (payload_len % frame_size) != 0) {
        return -EINVAL;
    }

    size_t frame_count = payload_len / frame_size;
    if (frame_count == 0 || frame_count > BCAST_MAX_FRAMES_PER_GROUP) {
        return -EINVAL;
    }

    PC_ARM_DATA_DATA *frames = (PC_ARM_DATA_DATA *)payload;

    for (size_t i = 0; i < frame_count; i++) {
        if (validate_48byte_can_payload(&frames[i]) != 0) {
            printf("[9012] 48-byte payload invalid at index %zu\n", i);
            return -EINVAL;
        }
    }

    if (frame_count == 1) {
        PC_ARM_DATA_DATA *frame = &frames[0];
        if (frame->type == 0x01) {
            return enqueue_normal_can_payload(frame);
        }
        if (frame->type == 0x02) {
            if (packet_channel >= AXICAN_MAX) {
                return -EINVAL;
            }
            return enqueue_broadcast_group((uint8_t)packet_channel, frame, 1);
        }

        return -EINVAL;
    }

    for (size_t i = 0; i < frame_count; i++) {
        if (frames[i].type != 0x02) {
            printf("[9012] multi 48-byte payload contains non-broadcast type 0x%02X at index %zu\n",
                   frames[i].type, i);
            return -EINVAL;
        }
    }

    if (packet_channel >= AXICAN_MAX) {
        return -EINVAL;
    }
    return enqueue_broadcast_group((uint8_t)packet_channel, frames, frame_count);
}

static void process_download_data_batch(const uint8_t *buffer, size_t total_len) {
    // 快速参数校验
    if (!buffer || total_len == 0) {
        printf("[9012 handler] Invalid input (buffer=%p, len=%zu)\n", buffer, total_len);
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, -EINVAL, "invalid input");
        return;
    }
    // 步骤1：高效处理暂存区（避免溢出与冗余拷贝）
    const size_t temp_buf_max = sizeof(g_temp_buf);
    if (g_temp_len + total_len > temp_buf_max) {
        printf("[9012 handler] Temp buffer overflow (current=%zu, new=%zu, max=%zu)\n",
               g_temp_len, total_len, temp_buf_max);
        // 仅保留最新数据（减少丢失窗口）
        size_t keep_len = temp_buf_max - total_len;
        printf("[9012 handler] Adjusting temp buffer - keep_len=%zu, discarding=%zu\n",
               keep_len, g_temp_len - keep_len);
        
        if (keep_len > 0 && g_temp_len > 0) {
            memmove(g_temp_buf, g_temp_buf + (g_temp_len - keep_len), keep_len);
            printf("[9012 handler] Moved %zu bytes to front of temp buffer\n", keep_len);
        }
        g_temp_len = keep_len;
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, -ENOMEM, "temp buffer overflow");
    }
    
    // 拷贝新数据到暂存区
    memcpy(g_temp_buf + g_temp_len, buffer, total_len);
    g_temp_len += total_len;
 

    // 步骤2：结构化解析数据包（减少重复计算）
    size_t offset = 0;
    size_t valid_count = 0, error_count = 0;
    const size_t req_header_size = sizeof(DATA_ReqPacket);
    const uint32_t expected_sync = PC_ARM_DATA_WORD;


    while (offset + req_header_size <= g_temp_len) {

        
        // 解析头部（避免重复指针转换）
        DATA_ReqPacket *req = (DATA_ReqPacket*)(g_temp_buf + offset);
        uint32_t req_sync_datelen=reverse_4bytes(req->data_len);
        uint32_t req_sync_datehead=reverse_4bytes(req->sync_word);
        const size_t pkg_total_len = req_header_size + reverse_4bytes(req->data_len);


        // 检查包完整性（提前退出条件）
        if (offset + pkg_total_len > g_temp_len) {
            // printf("[9012 parser] Incomplete packet at offset=%zu (needs=%zu, has=%zu) - stopping parse\n",
            //        offset, pkg_total_len, g_temp_len - offset);
            break;
        }

        // 同步字校验（快速失败）
        if (req_sync_datehead != expected_sync) {
            printf("[9012] Sync mismatch (offset=%zu, got=0x%08X, exp=0x%08X) - skipping byte\n",
                   offset, req->sync_word, expected_sync);
            offset++;
            error_count++;
            continue;
        }

        // 外层校验和（预计算长度常量）
        const uint32_t checksum_len = sizeof(req->sync_word) + sizeof(req->data_len) +
                                     sizeof(req->counter) + sizeof(req->cmd_type);
        const uint32_t actual_checksum = calculate_xor_checksum(g_temp_buf + offset, checksum_len);
        if (req->checksum != actual_checksum) {
            printf("[9012] Checksum mismatch (offset=%zu, got=0x%08X, exp=0x%08X) - skipping packet\n",
                   offset, req->checksum, actual_checksum);
            offset += pkg_total_len;
            error_count++;
            continue;
        }
        // 按命令类型处理（减少嵌套层级）
        uint16_t req_sync_datecmd=reverse_2bytes(req->cmd_type);
        const size_t payload_len = reverse_4bytes(req->data_len);
        uint32_t packet_channel = reverse_4bytes(req->channel);
        if ((req_sync_datecmd == 0x0001 || req_sync_datecmd == 0x0002) &&
            payload_len >= sizeof(PC_ARM_DATA_DATA) &&
            (payload_len % sizeof(PC_ARM_DATA_DATA)) == 0) {
            if (process_9012_can_payload_frames(req->data, payload_len, packet_channel) == 0) {
                valid_count++;
            } else {
                error_count++;
                send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, EINVAL, "9012 48-byte payload invalid");
            }
            offset += pkg_total_len;
            continue;
        }

        uint8_t can_id = 0;
        DataType data_type = DATA_TYPE_MAX;
        const void *data_ptr = req->data;
        bool valid = true;

        switch (req_sync_datecmd) {
            case 0x0001: { // 正常CAN数据
                PC_ARM_DATA_DATA *hdr = (PC_ARM_DATA_DATA*)data_ptr;
                
                if (hdr->head != PC_STATUS_HEAD) {
                    printf("[9012] CAN inner sync invalid (offset=%zu, val=0x%08X)\n", offset, hdr->head);
                    valid = false;
                    break;
                }
                
                const uint32_t crc_range = hdr->length - sizeof(hdr->xor_checksum);
                if (hdr->xor_checksum != calculate_xor_checksum((uint8_t*)hdr, crc_range)) {
                    printf("[9012] CAN inner checksum mismatch (offset=%zu, got=0x%08X)\n", 
                           offset, hdr->xor_checksum);
                    valid = false;
                    break;
                }
                
                can_id = hdr->ch_id;
                data_type = DATA_TYPE_CAN;
                // 通道有效性校验
                if (can_id >= AXICAN_MAX || !can_configs[can_id].valid) {
                    printf("[9012] Invalid CAN channel %d (max=%d, valid=%d)\n", 
                           can_id, AXICAN_MAX, can_configs[can_id].valid);
                    valid = false;
                }
                break;
            }

            case 0x0003: { // 模拟数据
                printf("[9012 parser] Processing simulator data packet (offset=%zu)\n", offset);
                PC_ARM_SIMULATOR_DATA *sim = (PC_ARM_SIMULATOR_DATA*)data_ptr;
                
                if (sim->head != PC_STATUS_HEAD) {
                    printf("sim->head:0x%08x along_data PC_STATUS_HEAD:0x%08x\n",sim->head,PC_STATUS_HEAD);
                    valid = false;
                    break;
                }
                
                can_id = sim->channel_id;
                data_type = DATA_TYPE_SIMULATOR;
                //printf("[9012 parser] Simulator data validated - channel=%d\n", can_id);
                break;
            }

            case 0x0002: { // 秒脉冲数据
                printf("[9012 parser] Processing second pulse data (offset=%zu)\n", offset);
                can_id = 0; // 固定通道
                data_type = DATA_TYPE_SECOND_PULSE;
                break;
            }

            case 0x0005: { // DMA数据
                printf("[9012 parser] Processing DMA data (offset=%zu)\n", offset);
                PC_DMA_DATA *dma = (PC_DMA_DATA*)data_ptr;
                can_id = dma->DMA_CHINNEL;
                data_type = DATA_TYPE_DMA;
                printf("[9012 parser] DMA data validated - channel=%d\n", can_id);
                break;
            }

            default:
                printf("[9012] Unknown cmd_type 0x%04X (offset=%zu)\n", req_sync_datecmd, offset);
                valid = false;
                break;
        }

        // 入队处理（根据数据类型选择缓冲区）
        if (valid && data_type != DATA_TYPE_MAX) {
            // 预计算元素大小
            const size_t elem_size = 
                (data_type == DATA_TYPE_CAN) ? sizeof(PC_ARM_DATA_DATA) :
                (data_type == DATA_TYPE_SIMULATOR) ? sizeof(PC_ARM_SIMULATOR_DATA) :
                (data_type == DATA_TYPE_SECOND_PULSE) ? sizeof(SecondPulseData) :
                sizeof(PC_DMA_DATA);
            // 按类型选择目标缓冲区
            RingBuffer32ChSet *target_buf = NULL;
            switch (data_type) {
                case DATA_TYPE_CAN: 
                    target_buf = &g_data_buf_set; 
                    break;
                case DATA_TYPE_SIMULATOR: 
                    target_buf = &g_simulator_buf_set; 
                    //printf("[9012 queue] Target buffer: Simulator data set\n");
                    break;
                case DATA_TYPE_SECOND_PULSE: 
                    target_buf = &g_pulse_buf_set; 
                    printf("[9012 queue] Target buffer: Pulse data set\n");
                    break;
                // DMA数据可根据需求添加对应缓冲区
                default: 
                    valid = false; 
                    printf("[9012 queue] No target buffer for data type %d\n", data_type);
                    break;
            }

            // 缓冲区入队（带容量检查）
            if (valid && target_buf) {
                // 提前检查缓冲区剩余空间
                size_t available = ringbuffer_32ch_avail(target_buf, can_id, 32);
                if (available == 0) {
                    printf("[9012 queue] Buffer full (ch=%d, type=%d) - no available space\n", 
                           can_id, data_type);
                    send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, -ENOMEM, "buffer full");
                    error_count++;
                } else if (ringbuffer_32ch_put(target_buf, can_id, data_ptr, 32) > 0) {
                    // printf("数据放进缓冲区\n\r");
                    valid_count++;
                } else {
                    printf("[9012 queue] Failed to enqueue (ch=%d, type=%d)\n", can_id, data_type);
                    error_count++;
                }
            } else {
                printf("[9012 queue] Invalid target buffer (ch=%d, type=%d)\n", can_id, data_type);
                error_count++;
            }
        } else {
           // printf("[9012 parser] Invalid data or unknown type (offset=%zu)\n", offset);
            error_count++;
        }

        offset += pkg_total_len;
    }
   
    // 高效处理剩余数据（减少内存移动）
    if (offset > 0) {
        if (offset < g_temp_len) {
            const size_t remaining = g_temp_len - offset;
            memmove(g_temp_buf, g_temp_buf + offset, remaining);
            g_temp_len = remaining;
        } else {
            g_temp_len = 0;
        }
    }
}


// 9012端口客户端处理线程
static void *tcp_client_handler_9012(void *client_socket) {
    int sock = *(int *)client_socket;
    free(client_socket);
    uint8_t buffer[BUFFER_SIZE_1];
    ssize_t bytes_read;
    
    printf("New data download client connected on port %d, socket: %d\n", 
           DATA_DOWNLOAD_PORT, sock);
    
    pthread_mutex_lock(&data_download_mutex);
    if (data_download_socket != -1) {
        close(data_download_socket);
        printf("Replaced existing download client on port %d\n", DATA_DOWNLOAD_PORT);
    }
    data_download_socket = sock;
    pthread_mutex_unlock(&data_download_mutex);
    
    while (running) {
        // 读取TCP数据（可能包含多个DATA_ReqPacket）
        bytes_read = recv(sock, buffer, BUFFER_SIZE_1, 0);
        if (bytes_read <= 0) {
            if (bytes_read < 0) {
                perror("recv failed in 9012 handler");
            }
            break;
        }
        //Can_Byte += bytes_read;
        process_download_data_batch(buffer, bytes_read);
        // // 更新TCP缓冲区使用量
        // update_tcp_buffer_usage(bytes_read);
        
        // process_download_data_batch(buffer, bytes_read);
       
    }
    
    pthread_mutex_lock(&data_download_mutex);
    if (data_download_socket == sock) {
        data_download_socket = -1;
    }
    pthread_mutex_unlock(&data_download_mutex);
    printf("Data download client disconnected on port %d\n", DATA_DOWNLOAD_PORT);
    close(sock);
    return NULL;
}
// 9012端口监听线程
static void *tcp_listen_thread_9012(void *port_ptr) {
    int port = *(int *)port_ptr;
    free(port_ptr);
    int server_fd, *new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    pthread_t client_tid;
    
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed on port 9012");
        return NULL;
    }
    
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt on port 9012");
        close(server_fd);
        return NULL;
    }
    
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
    
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed on port 9012");
        close(server_fd);
        return NULL;
    }
    
    if (listen(server_fd, MAX_CLIENTS) < 0) {
        perror("listen failed on port 9012");
        close(server_fd);
        return NULL;
    }
    
    printf("TCP server listening on data download port %d\n", port);
    
    while (running) {
        if ((new_socket = malloc(sizeof(int))) == NULL) {
            perror("malloc failed");
            continue;
        }
        
        if ((*new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept on port 9012");
            free(new_socket);
            continue;
        }
        
        if (pthread_create(&client_tid, NULL, tcp_client_handler_9012, new_socket) != 0) {
            perror("pthread_create failed for port 9012");
            close(*new_socket);
            free(new_socket);
        }
        pthread_detach(client_tid);
    }
    
    close(server_fd);
    return NULL;
}

// 9010端口客户端处理线程
static void *tcp_client_handler_9010(void *client_socket) {
    int sock = *(int *)client_socket;
    free(client_socket);
    
    printf("New data upload client connected on port %d, socket: %d\n", TCP_PORT2, sock);
    
    pthread_mutex_lock(&data_upload_mutex);
    if (data_upload_socket != -1) {
        close(data_upload_socket);
        printf("Replaced existing data client on port %d\n", TCP_PORT2);
    }
    data_upload_socket = sock;
    pthread_mutex_unlock(&data_upload_mutex);
    
    uint8_t buffer[1];
    while (running) {
        ssize_t ret = recv(sock, buffer, 1, 0);
        if (ret <= 0) {
            break;
        }
    }
    
    pthread_mutex_lock(&data_upload_mutex);
    if (data_upload_socket == sock) {
        data_upload_socket = -1;
    }
    pthread_mutex_unlock(&data_upload_mutex);
    
    printf("Data upload client disconnected on port %d, socket: %d\n", TCP_PORT2, sock);
    close(sock);
    return NULL;
}

// 9010端口监听线程
static void *tcp_listen_thread_9010(void *port_ptr) {
    int port = *(int *)port_ptr;
    free(port_ptr);
    int server_fd, *new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    pthread_t client_tid;
    
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed on port 9010");
        return NULL;
    }
    
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt on port 9010");
        close(server_fd);
        return NULL;
    }
    
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
    
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed on port 9010");
        close(server_fd);
        return NULL;
    }
    
    if (listen(server_fd, MAX_CLIENTS) < 0) {
        perror("listen failed on port 9010");
        close(server_fd);
        return NULL;
    }
    
    printf("TCP server listening on data upload port %d\n", port);
    
    while (running) {
        if ((new_socket = malloc(sizeof(int))) == NULL) {
            perror("malloc failed");
            continue;
        }
        
        if ((*new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept on port 9010");
            free(new_socket);
            continue;
        }
        
        if (pthread_create(&client_tid, NULL, tcp_client_handler_9010, new_socket) != 0) {
            perror("pthread_create failed for port 9010");
            close(*new_socket);
            free(new_socket);
        }
        pthread_detach(client_tid);
    }
    
    close(server_fd);
    return NULL;
}

// 9011端口客户端处理线程
static void *tcp_client_handler_9011(void *client_socket) {
    int sock = *(int *)client_socket;
    free(client_socket);
    
    printf("New event client connected on port %d, socket: %d\n", TCP_PORT3, sock);
    
    pthread_mutex_lock(&event_sock_mutex);
    if (event_socket != -1) {
        close(event_socket);
        printf("Replaced existing event client on port %d\n", TCP_PORT3);
    }
    event_socket = sock;
    pthread_mutex_unlock(&event_sock_mutex);
    
    uint8_t buffer[1];
    while (running) {
        ssize_t ret = recv(sock, buffer, 1, 0);
        if (ret <= 0) {
            break;
        }
    }
    
    pthread_mutex_lock(&event_sock_mutex);
    if (event_socket == sock) {
        event_socket = -1;
    }
    pthread_mutex_unlock(&event_sock_mutex);
    
    printf("Event client disconnected on port %d, socket: %d\n", TCP_PORT3, sock);
    close(sock);
    return NULL;
}

// 9011端口监听线程
static void *tcp_listen_thread_9011(void *port_ptr) {
    int port = *(int *)port_ptr;
    free(port_ptr);
    int server_fd, *new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    pthread_t client_tid;
    
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed on port 9011");
        return NULL;
    }
    
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt on port 9011");
        close(server_fd);
        return NULL;
    }
    
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
    
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed on port 9011");
        close(server_fd);
        return NULL;
    }
    
    if (listen(server_fd, MAX_CLIENTS) < 0) {
        perror("listen failed on port 9011");
        close(server_fd);
        return NULL;
    }
    
    printf("TCP server listening on event port %d\n", port);
    
    while (running) {
        if ((new_socket = malloc(sizeof(int))) == NULL) {
            perror("malloc failed");
            continue;
        }
        
        if ((*new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept on port 9011");
            free(new_socket);
            continue;
        }
        
        if (pthread_create(&client_tid, NULL, tcp_client_handler_9011, new_socket) != 0) {
            perror("pthread_create failed for port 9011");
            close(*new_socket);
            free(new_socket);
        }
        pthread_detach(client_tid);
    }
    
    close(server_fd);
    return NULL;
}

// 9009端口客户端处理线程
static void *tcp_client_handler_9009(void *client_socket) {
    int sock = *(int *)client_socket;
    free(client_socket);
    uint8_t buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    
    printf("New CAN command client connected on port %d, socket: %d\n", TCP_PORT1, sock);
    
    while (running) {
        bytes_read = recv(sock, buffer, BUFFER_SIZE, 0);
        if (bytes_read <= 0) {
            if (bytes_read < 0) {
                perror("recv from port 9009 error");
                send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,"9009 port recv failed" );
            }
            break;
        }
        
        // 9009原始报文dump较多，需要排查协议时再打开。
        // printf("Received %zd bytes on %d\n", bytes_read, TCP_PORT1);
        
        ReqPacket *req_packet;
        ParseStatus status = parse_request_packet(buffer, bytes_read, &req_packet);
        if (status != PARSE_SUCCESS) {
            const char *err_msg;
            switch (status) {
                case PARSE_ERR_SYNC: err_msg = "Invalid sync word"; break;
                case PARSE_ERR_LEN: err_msg = "Invalid packet length"; break;
                case PARSE_ERR_CHECKSUM: err_msg = "Checksum mismatch"; break;
                default: err_msg = "Unknown parse error";
            }
            //send_event(EVENT_LEVEL_FATAL, EVENT_CAN_INIT_FAILED, -1, "CAN controller initialization failed");
            continue;
        }
        uint16_t req_commond_type = reverse_2bytes(req_packet->cmd_type);
        switch ( req_commond_type) {
            case CMD_CAN_CONFIG:
                handle_can_config(sock, req_packet);
                break;
            case CMD_CAN_INFO_QUERY:
                handle_can_info_query(sock, req_packet);
                break;
            case CMD_CAN_BUF_QUERY:
                handle_can_buf_query(sock, req_packet);
                break;
            case CMD_CAN_START_STOP:
                handle_can_start_stop(sock, req_packet);
                break;
            case CMD_CAN_BROADCAST_CONFIG:
                handle_broadcast_config(sock, req_packet);
                break;
            default: {
                const char *err_msg = "Unknown CAN command";
                printf("Unknown CAN command: 0x%04X\n", req_packet->cmd_type);
                send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
                
                break;
            }
        }
    }
    
    // printf("CAN command client disconnected on port %d, socket: %d\n", TCP_PORT1, sock);
    close(sock);
    return NULL;
}

// 9009端口监听线程
static void *tcp_listen_thread_9009(void *port_ptr) {
    int port = *(int *)port_ptr;
    free(port_ptr);
    int server_fd, *new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    pthread_t client_tid;
    
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed on port 9009");
        return NULL;
    }
    
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt on port 9009");
        close(server_fd);
        return NULL;
    }
    
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
    
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed on port 9009");
        close(server_fd);
        return NULL;
    }
    
    if (listen(server_fd, MAX_CLIENTS) < 0) {
        perror("listen failed on port 9009");
        close(server_fd);
        return NULL;
    }
    
    printf("TCP server listening on CAN command port %d\n", port);
    
    while (running) {
        if ((new_socket = malloc(sizeof(int))) == NULL) {
            perror("malloc failed");
            continue;
        }
        
        if ((*new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept on port 9009");
            free(new_socket);
            continue;
        }
        
        if (pthread_create(&client_tid, NULL, tcp_client_handler_9009, new_socket) != 0) {
            perror("pthread_create failed for port 9009");
            close(*new_socket);
            free(new_socket);
        }
        pthread_detach(client_tid);
    }
    
    close(server_fd);
    return NULL;
}
static void* axican_data_process_thread_1(void* parameter)
{
    if (!parameter) 
    {
        printf("[出队线程] 无效参数，线程退出\n");
        return NULL;
    }
    
    // 获取CAN通道ID并释放参数内存
    int can_id = *(int*)parameter;
    free(parameter);

    cpu_set_t cpuset;
    CPU_SET(1, &cpuset);    
    pthread_t tid = pthread_self();
    int rc_c = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
    if (rc_c != 0) {
        printf("[CAN%d] 设置CPU亲和性失败: %d\n", can_id, rc_c);
    } else {
       // printf("[CAN%d] 线程已绑定到CPU核心: %d\n", can_id, can_id % sysconf(_SC_NPROCESSORS_ONLN));
    }
    
    // 设置线程优先级
    struct sched_param param_a;
    param_a.sched_priority = 80;  // 优先级值(1-99，值越大优先级越高)
    
    // 使用实时调度策略(SCHED_FIFO)
    rc_c = pthread_setschedparam(tid, SCHED_FIFO, &param_a);
    if (rc_c != 0) {
        printf("[CAN%d] 设置线程优先级失败: %d\n", can_id, rc_c);
    } else {
        //printf("[CAN%d] 线程优先级已设置为: %d\n", can_id, param_a.sched_priority);
    }
    
    // 批量处理配置
    struct axican_frame dequeue_batch[DEQUEUE_BATCH_SIZE];  // 从缓冲区读取的帧数组
    int actual_count = 0;                                   // 实际读取帧数
    uint32_t get_result;

    // printf("[CAN%d] 环形缓冲区处理线程启动（最大批量：%d帧)\n", can_id, DEQUEUE_BATCH_SIZE);

    while (running)
    {

        actual_count = 0;
        // 最多读取DEQUEUE_BATCH_SIZE帧，但不超过缓冲区中实际存在的帧数
        uint32_t available = ringbuffer_32ch_avail(&g_poll_buf_set, can_id, 6);
        uint32_t read_count = (available < DEQUEUE_BATCH_SIZE) ? 
                             available : DEQUEUE_BATCH_SIZE;

        for (int i = 0; i < read_count; i++)
        {
            // 从指定通道读取一帧数据
            get_result = ringbuffer_32ch_get(&g_poll_buf_set, can_id,&dequeue_batch[i],6);
            if (get_result == 1)
            {
                actual_count++;  // 读取成功，计数+1
            }
            else
            {
                // 读取失败（通常是缓冲区已空），退出循环
                break;
            }
        }
        if(can_configs[can_id].can_type == ALONG_MODE)
        {
            //82是CAN在单机模式情况下，收到的数据
            can_data_type=0x82;
        }
        else if(can_configs[can_id].can_type ==NORMAL_MODE)
        {
            //80是正常情况下，收到的数据
            can_data_type=0x80;
        }
        /* ---- 3. 处理读取到的帧数据 ---- */
        if (actual_count > 0)
        {
            /* 遍历处理每帧数据 */
            for (int i = 0; i < actual_count; i++)
            {
                struct axican_frame *can_frame = &dequeue_batch[i];
                send_can_transmit_response(can_id, can_data_type,1, can_frame);
            }
        }
        else
        {
            /* 缓冲区为空，短暂休眠减少CPU占用 */
            usleep(1000);
        }
    } /* end while(running) */

    printf("[CAN%d] 处理线程退出\n", can_id);
    return NULL;
}
static void* axican_data_process_thread(void* parameter)
{
    if (!parameter) 
    {
        printf("[出队线程] 无效参数，线程退出\n");
        return NULL;
    }
    
    // 获取CAN通道ID并释放参数内存
    int can_id = *(int*)parameter;
    free(parameter);
    
    // 批量处理配置
    struct axican_frame dequeue_batch[DEQUEUE_BATCH_SIZE];  // 从缓冲区读取的帧数组
    int actual_count = 0;                                   // 实际读取帧数
    uint32_t get_result;

    while (running)
    {
        while (running)
         {
        // 等待缓冲区满足条件（ALONG_MODE且帧数≥7）
            int is_single = (can_configs[can_id].can_type == NORMAL_MODE);
            int sim_ready = (can_configs[can_id].state==CAN_STATE_OPENED);
            if (is_single && sim_ready) break;
            usleep(1000);  // 1ms休眠，降低等待时CPU占用
        }
        /* ---- 2. 从环形缓冲区批量读取数据 ---- */
        actual_count = 0;
        // 最多读取DEQUEUE_BATCH_SIZE帧，但不超过缓冲区中实际存在的帧数
        uint32_t available = ringbuffer_32ch_avail(&g_can_buf_set, can_id, 6);
        uint32_t read_count = (available < DEQUEUE_BATCH_SIZE) ? 
                             available : DEQUEUE_BATCH_SIZE;

        for (int i = 0; i < read_count; i++)
        {
            // 从指定通道读取一帧数据
            get_result = ringbuffer_32ch_get(&g_can_buf_set, can_id,&dequeue_batch[i],6);
            if (get_result == 1)
            {
                actual_count++;  // 读取成功，计数+1
            }
            else
            {
                // 读取失败（通常是缓冲区已空），退出循环
                break;
            }
        }
        /* ---- 3. 处理读取到的帧数据 ---- */
        if (actual_count > 0)
        {
            /* 遍历处理每帧数据 */
            for (int i = 0; i < actual_count; i++)
            {
                struct axican_frame *can_frame = &dequeue_batch[i];
                
                handle_normal_mode(can_id, can_frame);
            }
        }
        else
        {
            /* 缓冲区为空，短暂休眠减少CPU占用 */
            usleep(1000);
        }
    } /* end while(running) */

    printf("[CAN%d] 处理线程退出\n", can_id);
    return NULL;
}
static int process_and_send_response(int can_id, int fd, int flags,PC_ARM_SIMULATOR_DATA *match_frame)
{

    // 入参有效性检查
    if (!match_frame || fd < 0) {
        printf("[CAN%d] 错误：无效参数(match_frame=%p, fd=%d)\n", can_id, match_frame, fd);
        return -1;
    }
     int rc;

    //总共可以回包会多少帧
    int total_frames = match_frame->send_len/ CAN_FRAME_DATA_LEN;
    printf("[CAN%d] 开始回包：总数据长度=%d字节,按%d字节/帧拆分，共需发送%d帧\n", 
           can_id, match_frame->send_len, CAN_FRAME_DATA_LEN, total_frames);
    
    // 1. 准备回包数据
    struct axican_frame response_frame;
   
    for (int frame_idx = 0; frame_idx < total_frames; frame_idx++)
    {
        memset(&response_frame, 0, sizeof(struct axican_frame));
        //计算出首地址偏移
        int data_offset = frame_idx * CAN_FRAME_DATA_LEN;
        memcpy(&response_frame,match_frame->send_data+frame_idx*16,16);
        struct timespec ts;
        if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
            response_frame.timestamp = (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
        } else {
            response_frame.timestamp = 0;
        }
        pthread_mutex_lock(&can_write_mutex[can_id]);
        rc = write(fd, &response_frame, sizeof(response_frame));
        pthread_mutex_unlock(&can_write_mutex[can_id]);
        if (rc != sizeof(response_frame)) {
            printf("[CAN%d发送线程] 错误：发送失败 - 写入 %d/%zu 字节\n",
                can_id, rc, sizeof(response_frame));
            send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, errno, "CAN along data send failed");
        } else {
         //send_can_transmit_response(can_id,0x82, 1, &response_frame);
        }
        printf("process_and_send_response write\n");
    }
    
    return 0;
}
//单机模拟线程处理
static void *along_data_tid_thread(void* parameter)
{
    int i, j, match_index = -1;
    struct axican *axican_info = (struct axican *)parameter;
    PC_ARM_SIMULATOR_DATA *matched_sim_frame = NULL; 
    
    // 1. 参数有效性检查
    if (!axican_info) {
        printf("[CAN%d] 无效的axican_info参数,线程退出\n", -1);
        return NULL;
    }
    
    int can_id = axican_info->id;
    int fd = axican_info->f_wr.fd;
    int flags = axican_info->f_wr.flags;

    // 2. 数据存储区定义
    // 2.1 单机模拟缓冲区数据（预加载，固定32帧上限）
    PC_ARM_SIMULATOR_DATA buffer_frames[32];  
    int buffer_frame_count = 0;               // 实际加载的缓冲区帧数
    
    // 2.2 队列数据存储（支持多帧命令缓存，需覆盖最大等待帧数）
    struct axican_frame dequeue_batch[DEQUEUE_BATCH_SIZE];  // 单次取帧缓存
    int queue_frame_count = 0;                              // 单次实际取帧数量
    
    // 2.3 多帧命令等待机制核心变量
    const int MAX_WAIT_FRAMES = 6; 
    struct axican_frame wait_cmd_frames[MAX_WAIT_FRAMES];   // 缓存等待中的命令帧
    int wait_frame_count = 0;                               // 当前已缓存的等待帧数
    int target_wait_frames = 0;                             // 匹配后需接收的总命令帧数
    int is_waiting = 0;                                     // 是否处于“等待后续命令帧”状态（0=否，1=是）


    // 3. 预加载：从单机模拟缓冲区读取数据（仅初始化时执行1次）
    while (running) {
        int is_single = (can_configs[can_id].can_type == ALONG_MODE);
        int sim_ready = (ringbuffer_32ch_len(&g_simulator_buf_set, can_id, 32) >= 7);
        if (is_single && sim_ready) break;
        usleep(100000);  // 1ms休眠，降低等待时CPU占用
    }

    // 读取缓冲区数据到本地缓存
    int counter = ringbuffer_32ch_len(&g_simulator_buf_set, can_id, 32);
    printf("#############CAN%d 单机缓冲区已有 %d 帧\n", can_id, counter);
    for (i = 0; i < counter && i < 32; i++) {  // 限制最大32帧，避免数组越界
        if (ringbuffer_32ch_get(&g_simulator_buf_set, can_id, &buffer_frames[i], 32) == 1) {
            //打印帧序号和基本信息
            printf("\n[CAN%d] 从单机缓冲区取数据：第%d帧 ---------------------\n", can_id, i+1);
            printf("头部标识: 0x%08X\n", buffer_frames[i].head);
            printf("方向: %hhu (0=PC→ARM, 1=ARM→PC)\n", buffer_frames[i].direction);
            printf("类型: 0x%02X\n", buffer_frames[i].type);
            printf("数据长度: %hu 字节\n", buffer_frames[i].length);
            printf("通道ID: %hhu\n", buffer_frames[i].channel_id);
            printf("总计数: %u\n", buffer_frames[i].total_count);
            printf("通道计数: %u\n", buffer_frames[i].channel_count);
            printf("包类型: 0x%04X\n", buffer_frames[i].packet_type);
            printf("CAN ID: 0x%08X\n", buffer_frames[i].can_id);
            printf("数据类型: 0x%08X\n", buffer_frames[i].data_type);
            printf("接收长度: %hu 字节\n", buffer_frames[i].rcv_len);
            printf("发送长度: %hu 字节\n", buffer_frames[i].send_len);
            printf("保留字段: 0x%02X\n", buffer_frames[i].reserved);
            
            //打印send_data字段（最多显示前16字节，避免输出过长）
            int print_len = (buffer_frames[i].send_len > 16) ? 16 : buffer_frames[i].send_len;
            printf("发送数据 (前%d字节): ", print_len);
            for (int k = 0; k < print_len; k++) {
                printf("%02X ", buffer_frames[i].send_data[k]);
            }
            if (buffer_frames[i].send_len > 16) {
                printf("... (还有%d字节未显示)", buffer_frames[i].send_len - 16);
            }
            printf("\n---------------------------------------------------\n");
            
            buffer_frame_count++;
        } else {
            printf("[CAN%d] 单机缓冲区取数中断，已取%d帧\n", can_id, buffer_frame_count);
            break;
        }
    }



    // 4. 主循环：处理队列命令帧（核心逻辑）
    while (running) {
        // 4.1 读取当前队列长度，避免无效循环
        int data_counter = ringbuffer_32ch_len(&g_can_buf_set, can_id, 6);
        if((can_id == 0) && (data_counter))
        {
            printf("#############CAN%d data_counter:%d\n", can_id, data_counter);
        }
        // printf("[CAN%d] 队列当前长度：%d | 等待状态：%s | 已缓存等待帧：%d\n", 
        //        can_id, data_counter, is_waiting ? "是" : "否", wait_frame_count);

        // 4.2 从队列读取数据（单次最多读DEQUEUE_BATCH_SIZE帧）
        queue_frame_count = 0;
        memset(dequeue_batch, 0, sizeof(dequeue_batch));  // 清空缓存，避免脏数据
        for (i = 0; i < data_counter && i < DEQUEUE_BATCH_SIZE; i++) {
            if (ringbuffer_32ch_get(&g_can_buf_set, can_id, &dequeue_batch[i], 6) == 1) {
                queue_frame_count++;
            } else {
                printf("[CAN%d] 队列取数中断，已取%d帧\n", can_id, queue_frame_count);
                break;
            }
        }

        // 4.3 处理队列取数结果（错误/无数据/有数据）
        if (queue_frame_count < 0) {
            // 取数错误：休眠10ms后重试
            printf("[CAN%d] 队列取出数据出错,休眠10ms\n", can_id);
            usleep(10000);
            continue;
        } else if (queue_frame_count == 0) {
            // 无数据：若处于等待状态，需等待后续帧；否则短休眠
            if (is_waiting) {
                //printf("[CAN%d] 等待后续命令帧,休眠5ms\n", can_id);
                usleep(5000);  // 等待时休眠稍长，避免频繁轮询
            } else {
                //printf("[CAN%d] ===============\n", can_id);
                usleep(1000);  // 非等待状态，1ms短休眠
            }
            continue;
        } else {
            // 有数据：打印取数信息
            //printf("[CAN%d] 从队列取出 %d 帧数据\n", can_id, queue_frame_count);
        }


        // 4.4 核心逻辑：处理取出的队列帧（分“等待状态”和“非等待状态”）
        for (i = 0; i < queue_frame_count; i++) {
            struct axican_frame *current_frame = &dequeue_batch[i];

            // --------------------------
            // 情况1：已处于“等待后续命令帧”状态（需接收第2帧）
            // --------------------------
            if (is_waiting) {
                // 缓存当前帧
                wait_cmd_frames[wait_frame_count++] = *current_frame;
               // printf("[CAN%d] 缓存等待帧（第%d帧/共需%d帧)\n", can_id, wait_frame_count, target_wait_frames+1);

                // 检查是否已缓存足够帧数：满足则执行处理，否则继续等待
                if (wait_frame_count >= target_wait_frames) {
                    printf("[CAN%d] 已接收全部命令帧（共%d帧),开始处理\n", 
                           can_id, target_wait_frames);
                    //  执行多帧命令处理（传入所有缓存的命令帧）
                    if (matched_sim_frame != NULL)
                    {
                        process_and_send_response(can_id, fd, flags, matched_sim_frame);
                    } 
                    else 
                    {
                        printf("[CAN%d] 警告：未找到匹配的单机缓冲区数据\n", can_id);
                    }


                    //如果是多帧的话，将第一帧匹配上的数据给缓冲区，然后将从队列里面取到的所有帧返回给上位机
                    for(int i=0;i<wait_frame_count;i++)
                    {
                        send_can_transmit_response(can_id, 0x83,1, &wait_cmd_frames[i]);
                    }
                    
                    // 重置等待状态（处理完成，回到初始状态）
                    is_waiting = 0;
                    wait_frame_count = 0;
                    target_wait_frames = 0;
                    matched_sim_frame = NULL;  // 清除匹配帧引用
                    memset(wait_cmd_frames, 0, sizeof(wait_cmd_frames));  // 清空缓存

                }
                // 等待状态下，无需进入匹配逻辑，直接处理下一个帧
                break;
            }


            // --------------------------
            // 情况2：未处于等待状态（需对当前帧进行匹配）
            // --------------------------
            match_index = -1;
            target_wait_frames = 0;  // 重置目标等待帧数
            matched_sim_frame = NULL;  // 重置匹配帧

            // 与单机缓冲区数据匹配（CAN ID + data[0]数据类型）
            for (j = 0; j < buffer_frame_count; j++) {
                uint32_t current_can_id_high11 = (current_frame->can_id >> 21) & 0x7FF;
                //printf("current_can_id_high11:0x%08x####buffer_frames[%d].can_id:0x%08x\n",current_can_id_high11,j,buffer_frames[j].can_id);
                if ((current_can_id_high11 == buffer_frames[j].can_id )&& (current_frame->data[0] == buffer_frames[j].data_type)) {
                    match_index = j;
                    // 确定需接收的总命令帧数
                    matched_sim_frame = &buffer_frames[j];  // 保存匹配帧引用
                    target_wait_frames = determine_response_frames(&buffer_frames[j]);
                    //printf("[CAN%d] 找到匹配帧（缓冲区索引：%d),需接收命令帧总数：%d\n", can_id, j, target_wait_frames);
                    break;  // 找到匹配后退出循环，避免重复匹配
                }
            }

            // 处理匹配结果（匹配成功/失败）
            if (match_index == -1) {
                // 未匹配：丢弃当前帧
                printf("[CAN%d] 未找到匹配帧，丢弃队列帧（第%d帧)\n", can_id, i+1);
                continue;
            }

            // 匹配成功：根据需接收的命令帧数处理
            if (target_wait_frames == 0) {
                // 需1帧命令：直接处理当前帧
               // printf("[CAN%d] 需1帧命令,直接处理当前帧\n", can_id);
                //直接将数据以四字节发送给
                //通过9010端口，将数据帧发送给上位机
                send_can_transmit_response(can_id, 0x83,1, current_frame);
                //组包发送给上位机
                int ret = process_and_send_response(can_id,fd, flags , &buffer_frames[j]);
                
                continue;

            } else if (target_wait_frames >0 && target_wait_frames <= MAX_WAIT_FRAMES) {
                // 需2帧命令：缓存当前帧，进入等待状态
                printf("[CAN%d] 需%d帧命令,缓存当前帧,要等待第%d帧\n", can_id,target_wait_frames,wait_frame_count+1);
                is_waiting = 1;  // 标记为等待状态
                wait_cmd_frames[wait_frame_count] = *current_frame;  //缓存当前帧
                wait_frame_count += 1;  // 已缓存1帧
            } else {
                // 异常情况：帧数非法，丢弃当前帧
                printf("[CAN%d] 非法的目标命令帧数：%d,丢弃当前帧\n", can_id, target_wait_frames);
                continue;
            }
        }

        // 主循环休眠：避免CPU占用过高（1ms平衡实时性和资源占用）
        usleep(1000);
    }

    printf("[CAN%d] 线程退出(running标志置为false)\n", can_id);
    return NULL;
}
// static void *along_data_tid_thread(void* parameter)
// {
//     int i, j, match_index = -1;
//     struct axican *axican_info = (struct axican *)parameter;
//     if (!axican_info) {  // 检查参数有效性
//         printf("[CAN%d] 无效的axican_info参数,线程退出\n", -1);
//         return NULL;
//     }
    
//     int can_id = axican_info->id;
//     int fd = axican_info->f_wr.fd;
//     int flags = axican_info->f_wr.flags;

    
//     // 缓冲区数据存储
//     PC_ARM_SIMULATOR_DATA buffer_frames[32];  // 从单机模拟缓冲区取出的数据
//     int buffer_frame_count = 0;               // 缓冲区实际数据帧数
    
//     // 队列数据存储
//     struct axican_frame dequeue_batch[DEQUEUE_BATCH_SIZE];
//     int queue_frame_count = 0;                      // 队列实际数据帧数
    
//     // 回包配置参数（根据实际需求调整）
//     const int MAX_MATCH_FRAMES = 5;  // 最大匹配帧数
//     int required_response_frames = 0;  // 需要的回包帧数
    
//     /* 2. 等待单机模拟缓冲区满足条件 */
//     while (running) {
//         int is_single = (can_configs[can_id].can_type == ALONG_MODE);
//         int sim_ready = (ringbuffer_32ch_len(&g_simulator_buf_set, can_id,32) >= 7);

//         if (is_single && sim_ready) break;
//         usleep(1000);
//     }

//     //从单机模拟缓冲区里面取出数据
//     int counter = ringbuffer_32ch_len(&g_simulator_buf_set, can_id,32);
//     printf("#############缓冲区已有 %d 帧\n", counter);
//         // 1. 从单机模拟缓冲区取出数据并存储
//         for(i = 0; i < counter; i++)
//         {
//             if (ringbuffer_32ch_get(&g_simulator_buf_set, can_id, &buffer_frames[i],32) == 1)
//             {
//                 printf("从单机模拟缓冲区取数据：%d\n",i+1);
//                 buffer_frame_count++;
//             }
//             else
//             {
//                 break;
//             }
//         }
//         printf("[CAN%d] 从缓冲区取出 %d 帧数据\n", can_id, buffer_frame_count);

//     printf("######%d\n",running);
//     while(running)
//     {
//         int data_counter = ringbuffer_32ch_len(&g_can_buf_set, can_id,6);
//         printf("[CAN%d] 队列当前长度：%d\n", can_id, data_counter);
//         // match_index = -1;
//         // required_response_frames = 0;
//         for (int i = 0; i < data_counter; i++)
//         {
//             queue_frame_count = ringbuffer_32ch_get(&g_can_buf_set,can_id,&dequeue_batch[i],6);
//         }
//         if (queue_frame_count > 0)
//         {
//             printf("[CAN%d] 从队列取出 %d 帧数据\n", can_id, queue_frame_count);
//         }
//         else if (queue_frame_count < 0)
//         {
//             printf("[CAN%d] 队列取出数据出错\n", can_id);
//             usleep(10000);  // 出错时稍长休眠
//             continue;
//         }
//         if (buffer_frame_count > 0 && queue_frame_count > 0)
//         {
//             for (int i = 0; i < queue_frame_count; i++)
//             {
//                 required_response_frames = 0;
//                 struct axican_frame *current_frame = &dequeue_batch[i];
//                 match_index = -1;
                
//                 // 与缓冲区中的所有数据进行对比
//                 for (j = 0; j < buffer_frame_count; j++)
//                 {
//                     if (current_frame->can_id == buffer_frames[j].can_id && current_frame->data[0] == buffer_frames[j].data_type)
//                     {
//                         match_index = j;
//                         printf("[CAN%d] 找到匹配帧，索引: %d\n", can_id, j);
//                         required_response_frames = determine_response_frames(&buffer_frames[j]);
//                         break;
//                     }
//                 }
//                 // 处理匹配结果
//                 if (match_index != -1)
//                 {
                    
//                 }
//                 //找到之后
//             }
//         }
            

       

        


        
//         // 3. 处理数据：只有当两边都有数据时才进行对比
//         // if (buffer_frame_count > 0 && queue_frame_count > 0)
//         // {
//         //     // 取队列的第一帧数据与缓冲区数据对比
//         //     CanFrameNode* first_queue_frame = queue_nodes[0];
            
//         //     // 遍历缓冲区数据寻找匹配项
//         //     for (j = 0; j < buffer_frame_count; j++)
//         //     {
//         //         // 这里根据实际数据结构实现对比逻辑
//         //         // 示例：比较帧ID和数据长度，可根据实际需求扩展
//         //         if (buffer_frames[j].can_id == first_queue_frame->frame.can_id &&buffer_frames[j].data_type == first_queue_frame->frame.data[0])
//         //         {
//         //             match_index = j;
//         //             printf("[CAN%d] 找到匹配帧，索引: %d\n", can_id, j);
                    
//         //             // 根据匹配到的帧确定需要多少帧才回包
//         //             required_response_frames = determine_response_frames(&buffer_frames[j]);
//         //             break;
//         //         }
//         //     }
            
//         //     // 4. 如果找到匹配项，检查队列数据是否满足回包要求
//         //     if (match_index != -1 && required_response_frames >= 0)
//         //     {
//         //         printf("[CAN%d] 需要 %d 帧数据进行回包\n", can_id, required_response_frames);
                
//         //         // 检查队列中的数据是否满足回包所需帧数
//         //         if (queue_frame_count >= required_response_frames)
//         //         {
//         //             // 处理满足条件的回包数据
         //   if (process_and_send_response(can_id, fd, flags, &buffer_frames[match_index]) == 0)
//         //             {
//         //                 printf("[CAN%d] 成功发送回包数据\n", can_id);
//         //             }
//         //             else
//         //             {
//         //                 printf("[CAN%d] 回包数据发送失败\n", can_id);
//         //             }
//         //         }
//         //         else
//         //         {
//         //             printf("[CAN%d] 队列数据不足，需要 %d 帧，实际只有 %d 帧\n", 
//         //                    can_id, required_response_frames, queue_frame_count);
//         //             // 可选择将数据重新入队或做其他处理
//         //             // requeue_frames(&can_queues[can_id], queue_nodes, queue_frame_count);
//         //         }
//         //     }
//         //     else if (match_index == -1)
//         //     {
//         //         printf("[CAN%d] 未找到匹配帧，丢弃数据\n", can_id);

//         //     }
//         // }
        
//         // 5. 释放队列节点内存（假设节点需要手动释放）
//         // for (i = 0; i < queue_frame_count; i++)
//         // {
//         //     if (queue_nodes[i])
//         //     {
//         //         can_queue_destroy(queue_nodes[i]);  // 释放节点内存
//         //         queue_nodes[i] = NULL;
//         //     }
//         // }
        
//         // // 短暂休眠，避免CPU占用过高
//         // usleep(1000);
//     }
    
//     printf("[CAN%d] 单机模拟数据处理线程退出\n", can_id);
//     return NULL;
// }

static uint32_t select_broadcast_mask_on_pps(void) {
    uint32_t mask = 0;

    pthread_mutex_lock(&bcast_cfg_mutex);
    if (g_bcast_cfg.enable != BCAST_ENABLE) {
        pthread_mutex_unlock(&bcast_cfg_mutex);
        return 0;
    }

    switch (g_bcast_cfg.mode) {
        case BCAST_MODE_SIMULTANEOUS:
            mask = g_bcast_cfg.active_mask;
            break;

        case BCAST_MODE_POLLING: {
            uint32_t active = g_bcast_cfg.active_mask & BCAST_CAN_MASK;
            for (uint8_t step = 0; step < AXICAN_MAX; step++) {
                uint8_t ch = (g_bcast_cfg.poll_next_channel + step) % AXICAN_MAX;
                if (active & (1U << ch)) {
                    mask = (1U << ch);
                    g_bcast_cfg.poll_next_channel = (ch + 1) % AXICAN_MAX;
                    break;
                }
            }
            break;
        }

        case BCAST_MODE_AB_ALTERNATE:
            if (g_bcast_cfg.current_group == 0) {
                mask = g_bcast_cfg.group_a_mask;
                g_bcast_cfg.current_group = 1;
            } else {
                mask = g_bcast_cfg.group_b_mask;
                g_bcast_cfg.current_group = 0;
            }
            break;

        default:
            mask = 0;
            break;
    }

    pthread_mutex_unlock(&bcast_cfg_mutex);
    return mask & BCAST_CAN_MASK;
}

static void wake_broadcast_channels(uint32_t mask) {
    for (uint8_t ch = 0; ch < AXICAN_MAX; ch++) {
        if ((mask & (1U << ch)) == 0) {
            continue;
        }

        pthread_mutex_lock(&g_bcast_wake[ch].mutex);
        g_bcast_wake[ch].pending++;
        pthread_cond_signal(&g_bcast_wake[ch].cond);
        pthread_mutex_unlock(&g_bcast_wake[ch].mutex);
    }
}

static void *pps_irq_thread(void *arg) {
    (void)arg;

    int fd = open(PPS_IRQ_DEV_PATH, O_RDONLY);
    if (fd < 0) {
        printf("[PPS] open %s failed: %s\n", PPS_IRQ_DEV_PATH, strerror(errno));
        send_event(EVENT_LEVEL_ERROR, EVENT_CAN_INIT_FAILED, errno, "PPS irq device open failed");
        return NULL;
    }

    struct pollfd pfd;
    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    pfd.events = POLLIN | POLLRDNORM;

    printf("[PPS] irq thread started, device=%s\n", PPS_IRQ_DEV_PATH);
    while (running) {
        int ret = poll(&pfd, 1, 1000);
        if (ret == 0) {
            continue;
        }
        if (ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            printf("[PPS] poll failed: %s\n", strerror(errno));
            break;
        }

        if (pfd.revents & (POLLIN | POLLRDNORM)) {
            unsigned int irq_count = 0;
            ioctl(fd, ZMUAV_PL2PS_IRQ_GET_IRQ_COUNT, &irq_count);

            uint32_t wake_mask = select_broadcast_mask_on_pps();
            if (wake_mask != 0) {
            // printf("[PPS] irq=%u wake broadcast mask=0x%X\n", irq_count, wake_mask);
            wake_broadcast_channels(wake_mask);
            }
        }
    }

    close(fd);
    printf("[PPS] irq thread exited\n");
    return NULL;
}

static int broadcast_can_write_ready(uint8_t can_id, int timeout_ms) {
    if (can_id >= AXICAN_MAX || axican_test[can_id].f_wr.fd < 0) {
        return 0;
    }

    return axican_poll2(axican_test[can_id].f_wr.fd, AXICAN_WRITE_FLAG, timeout_ms) == 0;
}

static int send_broadcast_group_for_channel(uint8_t can_id, const BroadcastFrameGroup *group) {
    if (!group || can_id >= AXICAN_MAX) {
        return -EINVAL;
    }
    if (can_configs[can_id].state != CAN_STATE_OPENED) {
        printf("[BCAST] CAN%u closed, skip group %u\n", can_id, group->group_id);
        return -EINVAL;
    }

    uint16_t written_frames = 0;
    for (uint16_t i = 0; i < group->frame_count; i++) {
        // printf("[CAN%u BCAST TX] group=%u frame=%u id=0x%08X dlc=0x%08X data0=0x%08X data1=0x%08X ts=%lld\n",
        //        can_id, group->group_id, i,
        //        group->frames[i].can_id,
        //        group->frames[i].can_dlc,
        //        group->frames[i].data[0],
        //        group->frames[i].data[1],
        //        group->frames[i].timestamp);

        pthread_mutex_lock(&can_write_mutex[can_id]);
        int rc = write(axican_test[can_id].f_wr.fd, &group->frames[i], sizeof(struct axican_frame));
        pthread_mutex_unlock(&can_write_mutex[can_id]);

        if (rc != sizeof(struct axican_frame)) {
            printf("[BCAST] CAN%u send group %u frame %u failed: %d/%zu\n",
                   can_id, group->group_id, i, rc, sizeof(struct axican_frame));
            send_event(EVENT_LEVEL_ERROR, EVENT_CAN_SENT_FAILE, errno, "broadcast CAN send failed");
            return -EIO;
        }
        written_frames++;
    }

    // printf("[BCAST] CAN%u sent group %u, frames=%u blocking_write=%u\n",
    //        can_id, group->group_id, group->frame_count, written_frames);
    return 0;
}

static void *broadcast_can_send_thread(void *parameter) {
    struct axican *axican_info = (struct axican *)parameter;
    if (!axican_info) {
        return NULL;
    }

    uint8_t can_id = axican_info->id;
    pthread_t tid = pthread_self();
    struct sched_param param_a;
    // 广播优先级高于普通发送(80)，低于 TX 监控(99)。
    param_a.sched_priority = 90;
    if (pthread_setschedparam(tid, SCHED_FIFO, &param_a) != 0) {
        printf("[BCAST] CAN%u set priority failed: %s\n", can_id, strerror(errno));
    }

    while (running) {
        pthread_mutex_lock(&g_bcast_wake[can_id].mutex);
        while (running && g_bcast_wake[can_id].pending == 0) {
            pthread_cond_wait(&g_bcast_wake[can_id].cond, &g_bcast_wake[can_id].mutex);
        }
        if (!running) {
            pthread_mutex_unlock(&g_bcast_wake[can_id].mutex);
            break;
        }
        g_bcast_wake[can_id].pending--;
        pthread_mutex_unlock(&g_bcast_wake[can_id].mutex);

        if (can_configs[can_id].state != CAN_STATE_OPENED) {
            continue;
        }

        BroadcastFrameGroup group;
        memset(&group, 0, sizeof(group));
        if (ringbuffer_32ch_get(&g_bcast_group_buf_set, can_id, &group, AXICAN_MAX) != 1) {
            // printf("[BCAST] CAN%u woke but no broadcast group queued\n", can_id);
            continue;
        }

        send_broadcast_group_for_channel(can_id, &group);
    }

    printf("[BCAST] CAN%u broadcast thread exited\n", can_id);
    return NULL;
}

//监控CAN数据
static void *send_data_tid_thread(void* parameter)
{
    struct axican *axican_info = (struct axican *)parameter;
    int can_id = axican_info->id;
    int fd = axican_info->f_wr.fd;
    int flags = axican_info->f_wr.flags;

    // 将各路 TX 监控线程分散绑定到可用 CPU，避免全部拥挤在同一核。
    long cpu_count = sysconf(_SC_NPROCESSORS_ONLN);
    if (cpu_count < 1) {
        cpu_count = 1;
    }
    int target_cpu = can_id % cpu_count;
    cpu_set_t monitor_cpuset;
    CPU_ZERO(&monitor_cpuset);
    CPU_SET(target_cpu, &monitor_cpuset);
    int affinity_rc = pthread_setaffinity_np(
        pthread_self(), sizeof(monitor_cpuset), &monitor_cpuset
    );
    if (affinity_rc != 0) {
        fprintf(stderr, "[CAN%d TX监控] 绑定CPU%d失败: %s\n",
                can_id, target_cpu, strerror(affinity_rc));
    } else {
        printf("[CAN%d TX监控] 已绑定CPU%d\n", can_id, target_cpu);
    }

    // TX 监控队列需要及时排空，避免驱动发送 FIFO 积压后反压普通发送。
    // SCHED_FIFO 需要 root 或 CAP_SYS_NICE；设置失败时继续以普通调度策略运行。
    pthread_t tid = pthread_self();
    struct sched_param monitor_param;
    memset(&monitor_param, 0, sizeof(monitor_param));
    monitor_param.sched_priority = sched_get_priority_max(SCHED_FIFO);
    int sched_rc = pthread_setschedparam(tid, SCHED_FIFO, &monitor_param);
    if (sched_rc != 0) {
        fprintf(stderr,
                "[CAN%d TX监控] 设置SCHED_FIFO最高优先级失败: %s\n",
                can_id, strerror(sched_rc));
    } else {
        printf("[CAN%d TX监控] SCHED_FIFO优先级已设置为%d\n",
               can_id, monitor_param.sched_priority);
    }

    // 复用帧缓冲区（避免栈上重复分配）
    struct axican_frame recv_frame;
    memset(&recv_frame, 0, sizeof(struct axican_frame));
    while (running) {
        
        // 非阻塞轮询（超时时间短，提高响应速度）
        int rc = axican_poll2(fd, flags, POLL_TIMEOUT_MS);
        
        // 无论是否有数据，都尝试读取（axican_read_data_t内部会检查帧数量）
        if (!rc) { 
            // 直接传入栈上缓冲区，避免成员访问开销
            int read_count = axican_read_data_t(axican_info->f_wr.fd, axican_info->f_wr.flags, can_id, &axican_info->tx_frame,&g_can_buf_set,AXICAN_CHAN_MAX);
            if (read_count < 0) {
                fprintf(stderr, "[CAN%d] 读取错误 (code: %d)\n", can_id, read_count);
            }
            axican_info->count += read_count;
            // POLLOUT 在设备可写时可能持续就绪。99级实时线程空转会
            // 饿死广播/普通发送线程，因此无监控帧时主动让出 CPU。
            if (read_count == 0) {
                usleep(100);
            }
        }else if (rc < 0) {
            printf("[CAN%d接收线程] 错误:poll接收通道失败,err=%d(%s)\n", can_id, errno, strerror(errno));
        } 
        
    }
    return NULL;
}
static void *CAN_byte_thread(void *arg) {
    while (1)
    {
        sleep(1);
        printf("CAN_byte %d\n",Can_Byte);
    }
    
}
static void *buffer_report_thread(void *arg) {
    usleep(5000000);  // 初始延迟5秒
    while (running) {
        /* 构造应答包（栈变量即可） */
        MultiChannelBufResponse upload_data;  
        // 关键修改：从全局变量获取总容量，而非硬编码的BUFFER_SIZE
        uint32_t tcp_buf_total;
        pthread_mutex_lock(&tcp_buffer_mutex);  // 加锁保护总容量读取
        tcp_buf_total = g_tcp_buffer_total;
        pthread_mutex_unlock(&tcp_buffer_mutex);
        
        // 获取已使用量（原有逻辑保留，需加锁）
        uint32_t current_tcp_used;
        pthread_mutex_lock(&tcp_buffer_mutex);
        current_tcp_used = g_tcp_buffer_used;
        pthread_mutex_unlock(&tcp_buffer_mutex);
        upload_data.tcp_buffer_head =reverse_4bytes(ACK_SYNC_WORD);
        upload_data.tcp_buffer_length= reverse_4bytes(32*sizeof(CanBufRemainingResponse));
        upload_data.tcp_buffer_count=0;
        upload_data.tcp_buffer_commond= reverse_2bytes(0x0100);
        uint32_t checksum_range_len = sizeof(upload_data.tcp_buffer_head) + sizeof(upload_data.tcp_buffer_length) + 
                                 sizeof(upload_data.tcp_buffer_count) + sizeof(upload_data.tcp_buffer_commond);
    uint32_t expected_checksum = calculate_xor_checksum((const uint8_t *)&upload_data, checksum_range_len);
        upload_data.tcp_buffer_checksum = expected_checksum ;
        
        /* ---------- 1. 计算各通道余量 ---------- */
        for (uint8_t ch = 0; ch < AXICAN_MAX; ch++) {
            CanBufRemainingResponse *curr_ch = &upload_data.channels[ch];
            memset(curr_ch, 0, sizeof(CanBufRemainingResponse));
            // curr_ch->CAN_DATA_RE_HEAD=reverse_4bytes(ACK_SYNC_WORD);
            // curr_ch->CAN_DATA_RE_LENGTH=reverse_4bytes(sizeof(CanBufRemainingResponse)-16);
            // curr_ch->CAN_DATA_RE_COUNT=0;
            // curr_ch->CAN_DATA_RE_COMMOND=0;
            // curr_ch->CAN_DATA_RE_Checksum=0;
            curr_ch->channel = ch;
            
            
            // 其他缓冲区信息
            curr_ch->normal_buf_remaining = ringbuffer_avail(g_data_buf_set.channels[ch]);
            // 复用原秒脉冲余量字段，上报当前PPS广播组缓冲区剩余组数。
            curr_ch->pulse_buf_remaining = ringbuffer_avail(g_bcast_group_buf_set.channels[ch]);
            curr_ch->sim_buf_remaining = ringbuffer_avail(g_simulator_buf_set.channels[ch]);
            curr_ch->dmaddr_space=0xFFFFFFFF;
            curr_ch->vfifo_space=0xFFFFFFFF;
            curr_ch->axififo_space=0xFFFFFFFF;
        }
        
        for (uint8_t ch = AXICAN_MAX; ch < MAX_CHANNELS; ch++) {
            CanBufRemainingResponse *curr_ch = &upload_data.channels[ch];
            // curr_ch->CAN_DATA_RE_HEAD=reverse_4bytes(ACK_SYNC_WORD);
            // curr_ch->CAN_DATA_RE_LENGTH=reverse_4bytes(sizeof(CanBufRemainingResponse)-16);
            // curr_ch->CAN_DATA_RE_COUNT=0;
            // curr_ch->CAN_DATA_RE_COMMOND=0;
            // curr_ch->CAN_DATA_RE_Checksum=0;
            curr_ch->channel = ch;
            
            
            // 其他缓冲区信息
            curr_ch->normal_buf_remaining = 0xFFFFFFFF;
            curr_ch->pulse_buf_remaining = 0xFFFFFFFF;
            curr_ch->sim_buf_remaining = 0xFFFFFFFF;
            curr_ch->dmaddr_space=0xFFFFFFFF;
            curr_ch->vfifo_space=0xFFFFFFFF;
            curr_ch->axififo_space=0xFFFFFFFF;
        }
        
        /* ---------- 2. 通过 9010 端口发送 ---------- */
        pthread_mutex_lock(&data_download_mutex);
        if (data_download_socket != -1) {
            ssize_t n = send(data_download_socket, &upload_data, sizeof(MultiChannelBufResponse), 0);
            if (n != sizeof(MultiChannelBufResponse)) {
                perror("send buffer report");
                close(data_download_socket);
                data_download_socket = -1;
                printf("9012端口连接已断开,后续上报将暂停\n\r");
            }
        } else {
            // printf("9012端口无客户端连接,跳过本次数据发送\n\r");
        }
        pthread_mutex_unlock(&data_download_mutex);

        usleep(500);   /**/
    }
    return NULL;
}
// 信号处理函数
void sigint_handler(int sig) {
    running = 0;
    printf("\nReceived exit signal, shutting down...\n");
}
//建表
static const uint32_t max_frames_per_type[DATA_TYPE_MAX] = {
    [DATA_TYPE_CAN]          = 100000,       //表示开辟CAN内存缓冲区的大小
    [DATA_TYPE_SECOND_PULSE] = 100,       //表示开辟秒脉冲内存缓冲区的大小
    [DATA_TYPE_SIMULATOR]    =100,           //表示开辟单机模拟缓冲区的大小
    [DATA_TYPE_DMA]          = 1000,        //表示开辟DMA 缓冲区的大小
    [DATA_TYPE_1553B]        = 1000,        //表示开辟1553B缓冲区的大小
};

int main(void) {
    int ret, id;
    pthread_t recv_tid[AXICAN_MAX];
    pthread_t send_tid[AXICAN_MAX];
    pthread_t report_tid;
    pthread_t send_data_tid[AXICAN_MAX];
    pthread_t along_data_tid[AXICAN_MAX];
    pthread_t bcast_send_tid[AXICAN_MAX];
    pthread_t pps_tid;
    pthread_t send_pc_tid;
    pthread_t listen_tid[4];
    pthread_t Can_byte_tid;
    // pthread_t dequeue_tid[AXICAN_MAX];
    pthread_t dequeue_tid_1[AXICAN_MAX];   /* 第一组出队线程 */
    pthread_t dequeue_tid_2[AXICAN_MAX];   /* 第二组出队线程 */
    int ports[4] = {TCP_PORT1, TCP_PORT2, TCP_PORT3, TCP_PORT4};
    unsigned int baud, mode;
    int pps_thread_started = 0;
    
    // signal(SIGINT, sigint_handler);
    // signal(SIGTERM, sigint_handler);
    
    pthread_mutex_init(&event_sock_mutex, NULL);
    pthread_mutex_init(&data_upload_mutex, NULL);
    pthread_mutex_init(&data_download_mutex, NULL);
    pthread_mutex_init(&data_counter_mutex, NULL);
    pthread_mutex_init(&data_counter_mutex, NULL);
    pthread_mutex_init(&bcast_cfg_mutex, NULL);
    pthread_mutex_init(&bcast_group_id_mutex, NULL);

    for (int i = 0; i < AXICAN_MAX; i++) {
        pthread_mutex_init(&can_write_mutex[i], NULL);
        pthread_mutex_init(&g_bcast_wake[i].mutex, NULL);
        pthread_cond_init(&g_bcast_wake[i].cond, NULL);
        g_bcast_wake[i].pending = 0;
    }

    ret = ringbuffer_32ch_init(&g_data_buf_set, sizeof(PC_ARM_DATA_DATA), 65536*2,32);
    if (ret != 0) {
        perror("初始化核心数据环形缓冲区失败");
        return -1;
    }
    // 2. 初始化模拟器数据缓冲区（PC_ARM_SIMULATOR_DATA类型，每个通道容量512帧）
    ret = ringbuffer_32ch_init(&g_simulator_buf_set, sizeof(PC_ARM_SIMULATOR_DATA), 32,32);
    if (ret != 0) {
        perror("初始化模拟器数据环形缓冲区失败");
        ringbuffer_32ch_destory(&g_data_buf_set,32); // 回滚已初始化的缓冲区
        return -1;
    }
    // 3. 初始化秒脉冲数据缓冲区（SecondPulseData类型，每个通道容量2048帧）
    ret = ringbuffer_32ch_init(&g_pulse_buf_set, sizeof(SecondPulseData), 2048,32);
    if (ret != 0) {
        perror("初始化秒脉冲数据环形缓冲区失败");
        ringbuffer_32ch_destory(&g_data_buf_set,32);
        ringbuffer_32ch_destory(&g_simulator_buf_set,32);
        return -1;
    }
    //创建CAN接收数据正常数据的队列6个队列
    ret = ringbuffer_32ch_init(&g_can_buf_set,sizeof(struct axican_frame),32768,6);
    if (ret != 0) {
        perror("初始化CAN队列缓冲区失败");
        ringbuffer_32ch_destory(&g_data_buf_set,32); // 回滚已初始化的缓冲区
        return -1;
    }
    //创建CAN监控数据正常数据的队列6个队列
    ret = ringbuffer_32ch_init(&g_poll_buf_set,sizeof(struct axican_frame),32768,6);
    if (ret != 0) {
        perror("初始化CAN队列缓冲区失败");
        ringbuffer_32ch_destory(&g_data_buf_set,32); // 回滚已初始化的缓冲区
        return -1;
    }

    ret = ringbuffer_32ch_init(&g_bcast_group_buf_set, sizeof(BroadcastFrameGroup), BCAST_GROUP_QUEUE_DEPTH, AXICAN_MAX);
    if (ret != 0) {
        perror("初始化广播组缓冲区失败");
        ringbuffer_32ch_destory(&g_data_buf_set,32);
        ringbuffer_32ch_destory(&g_simulator_buf_set,32);
        ringbuffer_32ch_destory(&g_pulse_buf_set,32);
        ringbuffer_32ch_destory(&g_can_buf_set,6);
        ringbuffer_32ch_destory(&g_poll_buf_set,6);
        return -1;
    }

    printf("所有环形缓冲区初始化完成(核心数据:65536帧/通道,模拟器:32帧/通道,秒脉冲:2048帧/通道）\n");
    //CAN接收线程缓冲区
    for (int i = 0; i < AXICAN_MAX; i++) {
        can_queue_init(&can_queues[i], 100000);  // 每个队列最大10000帧
      //  printf("初始化CAN%d队列完成\n", i);
    }





    // 初始化CAN设备
    for (id = 0; id < AXICAN_MAX; id++) {
        memset(&axican_test[id], 0x0, sizeof(axican_test[id]));
        
        axican_test[id].id = id;
        axican_test[id].f_rd.fd = open(axican_drv_name[id], O_RDONLY);
        axican_test[id].f_wr.fd = open(axican_drv_name[id], O_WRONLY);
        
        if (axican_test[id].f_rd.fd < 0) {
            char err_msg[64];
            snprintf(err_msg, sizeof(err_msg), "CAN%d read open failed: %s", id, strerror(errno));
            send_event(EVENT_LEVEL_ERROR,EVENT_CAN_INIT_FAILED, errno, err_msg);
            
            printf("axican%d Open read failed with error: %s\n", id, strerror(errno));
            return -1;
        }
        if (axican_test[id].f_wr.fd < 0) {
            char err_msg[64];
            snprintf(err_msg, sizeof(err_msg), "CAN%d write open failed: %s", id, strerror(errno));
            send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
            printf("%s\n", err_msg);
            return -1;
        }
        
        axican_test[id].f_wr.flags = AXICAN_WRITE_FLAG;
        axican_test[id].f_rd.flags = AXICAN_READ_FLAG;
        
        baud = 500000;
        mode = ZMUAV_XCAN_MODE_LOOPBACK;
        can_configs[id].can_id = id;
        can_configs[id].baud_rate = baud;
        can_configs[id].mode = mode;
        can_configs[id].valid = 1;
        can_configs[id].state = CAN_STATE_CLOSED;
        can_configs[id].can_type = INIT_MODE;
        ret = axican_set_baud(axican_test[id].f_rd.fd, baud);
        if (ret) {
            char err_msg[64];
            snprintf(err_msg, sizeof(err_msg), "CAN%d set baud failed: %s", id, strerror(errno));
            send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );

            printf("%s\n", err_msg);
            return -1;
        }
        
        ret = axican_set_mode(axican_test[id].f_rd.fd, mode);
        if (ret) {
            char err_msg[64];
            snprintf(err_msg, sizeof(err_msg), "CAN%d set mode failed: %s", id, strerror(errno));
            send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
            printf("%s\n", err_msg);
            return -1;
        }
        
        ret = pthread_create(&recv_tid[id], NULL, axican_data_recv_thread, &axican_test[id]);
        if(ret != 0){
            char err_msg[64];
            snprintf(err_msg, sizeof(err_msg), "CAN%d create recv thread failed", id);
            send_event(EVENT_LEVEL_ERROR,EVENT_CAN_SENT_FAILE,errno,err_msg );
            printf("%s\n", err_msg);
            return -1;
        }
        
        ret = pthread_create(&send_tid[id], NULL, axican_data_send_thread, &axican_test[id]);
        if(ret != 0){
            printf("can send pthread_create error \n");
            return -1;
        }
        ret = pthread_create(&send_data_tid[id], NULL, send_data_tid_thread, &axican_test[id]);
        if(ret != 0){
            printf("can send pthread_create error \n");
            return -1;
        }
        //创建各路单机模拟线程
        ret = pthread_create(&along_data_tid[id], NULL, along_data_tid_thread, &axican_test[id]);
        if(ret != 0){
            printf("can send pthread_create error \n");
            return -1;
        }

        ret = pthread_create(&bcast_send_tid[id], NULL, broadcast_can_send_thread, &axican_test[id]);
        if(ret != 0){
            printf("broadcast send pthread_create error \n");
            return -1;
        }
    }
        
    // 创建TCP监听线程
    for (int i = 0; i < 4; i++) {
        int *port = malloc(sizeof(int));
        if (!port) {
            perror("Failed to allocate memory for port");
            continue;
        }
        *port = ports[i];
        
        if (ports[i] == TCP_PORT1) {
            ret = pthread_create(&listen_tid[i], NULL, tcp_listen_thread_9009, port);
        } else if (ports[i] == TCP_PORT2) {
            ret = pthread_create(&listen_tid[i], NULL, tcp_listen_thread_9010, port);
        } else if (ports[i] == TCP_PORT3) {
            ret = pthread_create(&listen_tid[i], NULL, tcp_listen_thread_9011, port);
        } else if (ports[i] == TCP_PORT4) {
            ret = pthread_create(&listen_tid[i], NULL, tcp_listen_thread_9012, port);
        }
        
        if (ret != 0) {
            perror("Failed to create TCP listen thread");
            free(port);
        }
    }
    //定时上报接收到的数据的大小
     //Can_Byte创建一个线程定时每1s上传Can_Byte的数据
    // if (pthread_create(&Can_byte_tid, NULL, CAN_byte_thread, NULL) == 0)
    // {
    //     pthread_detach(Can_byte_tid);
    // }
    //定时上报缓冲区的线程
    
    if (pthread_create(&report_tid, NULL, buffer_report_thread, NULL) == 0)
    {
        pthread_detach(report_tid);
    }

    if (pthread_create(&pps_tid, NULL, pps_irq_thread, NULL) != 0)
    {
        perror("Failed to create PPS irq thread");
    } else {
        pps_thread_started = 1;
    }
    //创建监控线程线程将读取队列发送给上位机
    for (int i = 0; i < AXICAN_MAX; ++i) {
        int *arg = malloc(sizeof(int));
        *arg = i;
        pthread_create(&dequeue_tid_1[i], NULL, axican_data_process_thread_1, arg);
    }

    /* 第二组：axican_data_process_thread */
    for (int i = 0; i < AXICAN_MAX; ++i) {
        int *arg = malloc(sizeof(int));
        *arg = i;
        pthread_create(&dequeue_tid_2[i], NULL, axican_data_process_thread, arg);
    }
    
    while (running) {
        sleep(1);
    }

    for (int id = 0; id < AXICAN_MAX; id++) {
        pthread_mutex_lock(&g_bcast_wake[id].mutex);
        pthread_cond_signal(&g_bcast_wake[id].cond);
        pthread_mutex_unlock(&g_bcast_wake[id].mutex);
    }
    
    // 清理资源
    for (int id = 0; id < AXICAN_MAX; id++) {
            if (pthread_join(recv_tid[id], NULL) != 0) {
                perror("pthread_join recv_tid failed");
            }
            if (pthread_join(send_tid[id], NULL) != 0) {
                perror("pthread_join send_tid failed");
            }
            if (pthread_join(along_data_tid[id], NULL) != 0) {
                perror("pthread_join along_data_tid failed");
            }
            if (pthread_join(send_data_tid[id], NULL) != 0) {
                perror("pthread_join send_data_tid failed");
            }
            if (pthread_join(bcast_send_tid[id], NULL) != 0) {
                perror("pthread_join bcast_send_tid failed");
            }
            
            // 关闭CAN设备文件描述符（线程已退出，不会再访问）
            close(axican_test[id].f_rd.fd);
            close(axican_test[id].f_wr.fd);
            printf("CAN%d device closed\n", id);
            
            // 释放CAN缓冲区内存（can_buffers）
            for (int type = 0; type < 3; type++) {
                if (can_buffers[id][type].frames) {
                    free(can_buffers[id][type].frames);
                    can_buffers[id][type].frames = NULL;  // 避免野指针
                }
            }
        }
    printf("All CAN threads exited and devices closed\n");
    /* 2. 等待两组出队线程 */
    for (int i = 0; i < AXICAN_MAX; ++i) {
        pthread_join(dequeue_tid_1[i], NULL);
        pthread_join(dequeue_tid_2[i], NULL);
    }

    
	    for (int i = 0; i < 4; i++) {
	        pthread_join(listen_tid[i], NULL);
	    }

    if (pps_thread_started) {
        pthread_join(pps_tid, NULL);
    }

    //销毁缓冲区
    destroy_all_ring_buffers();
	    
	    pthread_mutex_destroy(&event_sock_mutex);
	    pthread_mutex_destroy(&data_upload_mutex);
	    pthread_mutex_destroy(&data_download_mutex);
	    pthread_mutex_destroy(&data_counter_mutex);
    pthread_mutex_destroy(&bcast_cfg_mutex);
    pthread_mutex_destroy(&bcast_group_id_mutex);
    for (int i = 0; i < AXICAN_MAX; i++) {
        pthread_mutex_destroy(&can_write_mutex[i]);
        pthread_mutex_destroy(&g_bcast_wake[i].mutex);
        pthread_cond_destroy(&g_bcast_wake[i].cond);
    }
    
    printf("All resources released, exit successfully\n");
    return 0;
}
