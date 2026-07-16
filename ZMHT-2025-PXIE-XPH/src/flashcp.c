#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <setjmp.h>
#include <signal.h>
#include <paths.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stddef.h>
#include <string.h>
#include <libgen.h> 
#include <poll.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <termios.h>
#include <time.h>
#include <sys/param.h>
#include <pwd.h>
#include <grp.h>
#include <unistd.h>
#include <mtd/mtd-user.h>

#include "flashcp.h"

// 统一数据类型：适配64位地址
typedef uint64_t uoff_t;
#define OFF_FMT "ll"  // 适配uint64_t的格式化输出

#define BUFSIZE (64 * 1024)  // 64KB块大小
#define MAX_RETRY_CNT 10000  // 最大中断重试次数

// 静态缓冲区宏（保留原逻辑）
#define RESERVE_CONFIG_UBUFFER(buffer,len) static unsigned char buffer[len]

/**
 * @brief 刷新所有文件缓冲区
 */
int fflush_all(void)
{
    return fflush(NULL);
}

/**
 * @brief 安全打开文件，带错误打印
 * @param pathname 路径
 * @param flags 打开标志
 * @return 文件描述符，失败返回-1
 */
int xopen(const char *pathname, int flags)
{
    if (pathname == NULL) {
        printf("xopen: path is NULL \n");
        return -1;
    }

    // MTD设备建议用O_SYNC|O_RDWR，文件用O_RDONLY
    mode_t mode = (flags & O_RDWR) ? 0600 : 0;
    int ret = open(pathname, flags, mode);
    if (ret < 0) {
        printf("xopen: can't open '%s' (errno=%d) \n", pathname, errno);
    }
    return ret;
}

/**
 * @brief 安全移动文件指针，带错误打印
 * @param fd 文件描述符
 * @param offset 偏移量
 * @param whence 偏移基准（SEEK_SET/SEEK_CUR/SEEK_END）
 * @return 新偏移量，失败返回-1
 */
off_t xlseek(int fd, off_t offset, int whence)
{
    off_t off = lseek(fd, offset, whence);
    if (off == (off_t)-1) {
        printf("xlseek: fd=%d, offset=%"OFF_FMT"d, whence=%d (errno=%d) \n", 
               fd, offset, whence, errno);
    }
    return off;
}

/**
 * @brief 安全读：处理EINTR中断，带重试限制
 * @param fd 文件描述符
 * @param buf 缓冲区
 * @param count 读取长度
 * @return 实际读取长度，失败返回-1
 */
ssize_t safe_read(int fd, void *buf, size_t count)
{
    ssize_t n;
    int err_cnt = 0;

    if (buf == NULL || count == 0) {
        printf("safe_read: invalid param \n");
        return -1;
    }

    for (;;) {
        n = read(fd, buf, count);
        if (n >= 0 || errno != EINTR) {
            break;
        }

        // 处理EINTR中断，重置errno并重试
        errno = 0;
        err_cnt++;
        if (err_cnt > MAX_RETRY_CNT) {
            printf("safe_read: retry %d times failed (fd=%d) \n", err_cnt, fd);
            return -1;
        }
    }

    return n;
}

/**
 * @brief 安全写：处理EINTR中断，带重试限制
 * @param fd 文件描述符
 * @param buf 数据缓冲区
 * @param count 写入长度
 * @return 实际写入长度，失败返回-1
 */
ssize_t safe_write(int fd, const void *buf, size_t count)
{
    ssize_t n;
    int err_cnt = 0;

    if (buf == NULL || count == 0) {
        printf("safe_write: invalid param \n");
        return -1;
    }

    for (;;) {
        n = write(fd, buf, count);
        if (n >= 0 || errno != EINTR) {
            break;
        }

        // 处理EINTR中断，重置errno并重试
        errno = 0;
        err_cnt++;
        if (err_cnt > MAX_RETRY_CNT) {
            printf("safe_write: retry %d times failed (fd=%d) \n", err_cnt, fd);
            return -1;
        }
    }

    return n;
}

/**
 * @brief 完整写：保证写入指定长度（处理短写）
 * @param fd 文件描述符
 * @param buf 数据缓冲区
 * @param len 待写入长度
 * @return 实际写入长度，失败返回-1
 */
ssize_t full_write(int fd, const void *buf, size_t len)
{
    ssize_t cc;
    ssize_t total = 0;

    while (len > 0) {
        cc = safe_write(fd, buf, len);
        if (cc < 0) {
            return (total > 0) ? total : cc;
        }
        if (cc == 0) {
            break;
        }

        total += cc;
        buf = (const char *)buf + cc;
        len -= cc;
    }

    return total;
}

/**
 * @brief 完整读：保证读取指定长度（处理短读）
 * @param fd 文件描述符
 * @param buf 缓冲区
 * @param len 待读取长度
 * @return 实际读取长度，失败返回-1
 */
ssize_t full_read(int fd, void *buf, size_t len)
{
    ssize_t cc;
    ssize_t total = 0;

    while (len > 0) {
        cc = safe_read(fd, buf, len);
        if (cc < 0) {
            return (total > 0) ? total : cc;
        }
        if (cc == 0) {
            break;
        }

        total += cc;
        buf = (char *)buf + cc;
        len -= cc;
    }

    return total;
}

/**
 * @brief 强制读：必须读取指定长度，否则报错
 * @param fd 文件描述符
 * @param buf 缓冲区
 * @param count 读取长度
 */
void xread(int fd, void *buf, size_t count)
{
    if (count == 0) {
        return;
    }

    ssize_t size = full_read(fd, buf, count);
    if ((size_t)size != count) {
        printf("xread: short read (expect %zu, actual %zd) \n", count, size);
    }
}

/**
 * @brief 打印烧写进度
 * @param mode -1=擦除，0=写入，1=校验
 * @param count 已完成数量（块数）
 * @param total 总数量（块数）
 */
static void progress(int mode, uoff_t count, uoff_t total)
{
    if (total == 0) {
        return;
    }

    uoff_t percent = (count * 100) / total;
    const char *mode_str = NULL;
    switch (mode) {
        case -1: mode_str = "Erasing"; break;
        case 0:  mode_str = "Writing"; break;
        case 1:  mode_str = "Verifying"; break;
        default: mode_str = "Processing";
    }

    // 覆盖式打印进度（\r回到行首）
    printf("\r%s: %"OFF_FMT"u/%"OFF_FMT"u blocks (%u%%)",
           mode_str, count, total, (unsigned)percent);
    fflush_all();
}

/**
 * @brief 核心烧写函数：擦除→写入→校验
 * @param info 设备/数据信息
 * @return 0成功，-1打开失败，-2非MTD设备，-3擦除失败，-4写入失败，-5校验失败
 */
int flashcp_main(device_info_t* info)
{
    // 1. 空指针检查
    if (info == NULL || info->devicename == NULL || info->buff == NULL) {
        printf("flashcp_main: invalid param (info=%p, dev=%p, buff=%p) \n",
               info, info ? info->devicename : NULL, info ? info->buff : NULL);
        return -1;
    }
    if (info->size == 0) {
        printf("flashcp_main: data size is 0 \n");
        return -1;
    }

    int fd_d = -1;
    int ret = 0;
    uoff_t erase_count = 0;
    struct mtd_info_user mtd;
    struct erase_info_user e;

    // 2. 静态缓冲区（64KB）
    RESERVE_CONFIG_UBUFFER(buf, BUFSIZE);
    RESERVE_CONFIG_UBUFFER(buf2, BUFSIZE);

    printf("flashcp_main: dev=%s, data size=%u bytes \n", info->devicename, info->size);

    // 3. 打开MTD设备
    fd_d = xopen(info->devicename, O_SYNC | O_RDWR);
    if (fd_d < 0) {
        ret = -1;
        goto error;
    }

    // 4. 检查是否为MTD设备
    if (ioctl(fd_d, MEMGETINFO, &mtd) < 0) {
        printf("flashcp_main: %s is not a MTD flash device (errno=%d) \n",
               info->devicename, errno);
        ret = -2;
        goto error;
    }

    // 5. 检查数据长度是否超过Flash容量
    if (info->size > mtd.size) {
        printf("flashcp_main: data size(%u) > flash size(%u) \n", info->size, mtd.size);
        ret = -2;
        goto error;
    }

    // 6. 擦除Flash（按擦除块大小对齐）
    erase_count = (uoff_t)(info->size + mtd.erasesize - 1) / mtd.erasesize;
    e.length = mtd.erasesize;
    e.start = 0;

    printf("flashcp_main: erase %"OFF_FMT"u blocks (erase size=%u) \n", erase_count, mtd.erasesize);
    for (uoff_t i = 1; i <= erase_count; i++) {
        progress(-1, i, erase_count);

        if (ioctl(fd_d, MEMERASE, &e) < 0) {
            printf("\nflashcp_main: erase error at 0x%llx (errno=%d) \n",
                   (unsigned long long)e.start, errno);
            ret = -3;
            goto error;
        }
        e.start += mtd.erasesize;
    }
    printf("\n");

    // 7. 写入+校验阶段（i=0写入，i=1校验）
    for (int i = 0; i <= 1; i++) {
        uoff_t done = 0;
        unsigned int count = BUFSIZE;
        uoff_t total_blocks = (info->size + BUFSIZE - 1) / BUFSIZE;

        xlseek(fd_d, 0, SEEK_SET);  // 回到Flash起始位置

        while (1) {
            uoff_t rem = info->size - done;
            if (rem == 0) {
                break;
            }

            // 适配最后一块的长度（不足64KB）
            if (rem < BUFSIZE) {
                count = rem;
            }

            // 打印进度
            progress(i, done / BUFSIZE + 1, total_blocks);

            // 从内存缓冲区拷贝数据到本地buf
            memcpy(buf, info->buff + done, count);

            if (i == 0) {
                // 写入阶段：不足64KB的部分用0xff填充（Flash擦除后默认值）
                if (count < BUFSIZE) {
                    memset((char*)buf + count, 0xff, BUFSIZE - count);
                }

                // 写入Flash
                ssize_t w_len = full_write(fd_d, buf, BUFSIZE);
                if (w_len != BUFSIZE) {
                    printf("\nflashcp_main: write error at 0x%"OFF_FMT"x (write %zd/%zu) \n",
                           done, w_len, BUFSIZE);
                    ret = -4;
                    goto error;
                }
            } else {
                // 校验阶段：读取Flash数据并对比
                xread(fd_d, buf2, count);
                if (memcmp(buf, buf2, count) != 0) {
                    printf("\nflashcp_main: verify mismatch at 0x%"OFF_FMT"x \n", done);
                    ret = -5;
                    goto error;
                }
            }

            done += count;
        }
        printf("\n");
    }

    // 8. 成功完成
    printf("flashcp_main: upgrade success! \n");
    ret = 0;

error:
    // 关闭文件描述符（仅当fd有效时）
    if (fd_d >= 0) {
        close(fd_d);
    }
    return ret;
}