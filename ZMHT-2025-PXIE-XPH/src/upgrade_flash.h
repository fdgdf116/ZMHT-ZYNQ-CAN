#ifndef __UPGRADE_FLASH_H__
#define __UPGRADE_FLASH_H__

#include <stdint.h>
#include <stddef.h>

// -------------------------- 宏定义 --------------------------
// 网络数据包相关
#define NET_PKG_DATA_SIZE_MAX    4096U    // 单包最大数据长度（字节）
#define FLASH_SECTOR_SIZE        4096U    // Flash扇区大小（根据硬件修改）

// Flash地址相关（根据实际硬件手册调整）
#define FLASH_FPGA_BIT_ADDR      0x00000000U  // FPGA比特流起始地址
#define FLASH_DTB_ADDR           0x00800000U  // DTB文件起始地址
#define FLASH_APP_ADDR           0x01000000U  // APP文件起始地址
#define FLASH_ROOTFS_ADDR        0x02000000U  // 根文件系统起始地址
#define FLASH_IMAGE_ADDR         0x04000000U  // 镜像文件起始地址
#define FLASH_BOOTBIN_ADDR       0x08000000U  // BOOTBIN起始地址

#define FLASH_MAX_ADDR           0x10000000U  // Flash总容量（256MB，示例）

// -------------------------- 枚举定义 --------------------------
/**
 * @brief 升级类型枚举（统一管理，避免魔法值）
 */
typedef enum {
    UPGRADE_TYPE_FPGA      = 0x01,    // FPGA比特流升级
    UPGRADE_TYPE_DTB       = 0x02,    // DTB文件升级
    UPGRADE_TYPE_APP       = 0x03,    // APP应用升级
    UPGRADE_TYPE_ROOTFS    = 0x04,    // 根文件系统升级
    UPGRADE_TYPE_IMAGE     = 0x05,    // 系统镜像升级
    UPGRADE_TYPE_BOOTBIN   = 0x06,    // BOOTBIN升级
    UPGRADE_TYPE_INVALID   = 0xFF     // 无效升级类型
} UpgradeType;

// -------------------------- 结构体定义 --------------------------
/**
 * @brief 网络升级数据包结构体
 */
typedef struct {
    uint32_t type;                // 升级类型（对应UpgradeType）
    uint32_t total_num;           // 总包数
    uint32_t pkg_num;             // 当前包序号（从0开始）
    uint32_t valid_len;           // 本包有效数据长度
    uint8_t  pkg_data[NET_PKG_DATA_SIZE_MAX];  // 数据包体
} netpkg_info_t;

/**
 * @brief 请求包结构体（网络层传入）
 */
typedef struct {
    uint8_t* data;                // 请求数据（指向netpkg_info_t）
    uint32_t crc32;               // 数据CRC32校验值
    uint32_t length;              // 数据总长度
    uint32_t count;               // 请求计数（标识会话）
    uint32_t index;               // 请求索引（标识包序）
} ReqPacket;

/**
 * @brief 响应包结构体（返回给网络层）
 */
typedef struct {
    uint8_t* data;                // 响应数据
    uint32_t crc32;               // 响应CRC32
    uint32_t count;               // 对应请求的count
    uint32_t index;               // 对应请求的index
} RespPacket;

/**
 * @brief Flash地址映射表（升级类型与Flash地址绑定）
 */
typedef struct {
    UpgradeType type;             // 升级类型
    uint32_t    flash_base_addr;  // Flash起始地址
    uint32_t    max_size;         // 该区域最大容量（防止越界）
} FlashAddrMap;

/**
 * @brief 全局升级上下文（管理升级过程状态）
 */
typedef struct {
    UpgradeType type;             // 当前升级类型
    uint32_t    total_pkgs;       // 总包数
    uint32_t    recv_pkgs;        // 已接收包数
    uint32_t    total_size;       // 总数据长度
    uint8_t*    data_buff;        // 接收数据缓冲区
    uint32_t    flash_addr;       // 目标Flash起始地址
} UpgradeCtx;

// -------------------------- 全局变量声明 --------------------------
// 升级上下文（extern声明，在.c文件中定义）
extern UpgradeCtx g_upgrade_ctx;

// Flash地址映射表（extern声明，在.c文件中初始化）
extern const FlashAddrMap flash_map[];
extern const size_t flash_map_size;

// -------------------------- 函数声明 --------------------------
/**
 * @brief 根据升级类型获取对应的Flash起始地址
 * @param type 升级类型（UpgradeType）
 * @return Flash起始地址（0表示类型错误）
 */
uint32_t get_flash_addr_by_type(UpgradeType type);

/**
 * @brief Flash擦除函数（底层驱动接口）
 * @param addr Flash起始地址
 * @param len  擦除长度（字节，需对齐扇区）
 * @return 0成功，非0失败
 */
int flash_erase(uint32_t addr, uint32_t len);

/**
 * @brief Flash写入函数（底层驱动接口）
 * @param addr Flash起始地址
 * @param data 待写入数据
 * @param len  写入长度（字节）
 * @return 0成功，非0失败
 */
int flash_write(uint32_t addr, const uint8_t* data, uint32_t len);

/**
 * @brief 接收升级数据包（逐包处理）
 * @param req 网络请求包
 * @return 0成功，非0失败（1=类型错，2=包序错，3=内存不足）
 */
int recv_upgrade_pkg(ReqPacket* req);

/**
 * @brief 将完整升级数据写入Flash
 * @return 0成功，非0失败（1=擦除失败，2=写入失败，3=校验失败）
 */
int write_upgrade_data_to_flash(void);

/**
 * @brief 升级主处理函数（对接网络层）
 * @param req 网络请求包
 */
void fpga_main_upgrage(ReqPacket* req);

/**
 * @brief 发送升级响应包（返回处理结果）
 * @param req  请求包（用于填充响应标识）
 * @param ret  处理结果（0=成功，非0=失败）
 */
void send_upgrade_response(ReqPacket* req, int ret);

#endif  // __UPGRADE_FLASH_H__