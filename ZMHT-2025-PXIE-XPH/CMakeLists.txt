#指定cmake 最低版本
cmake_minimum_required(VERSION 3.10)

# 指定交叉编译器（aarch64 架构，64位 ARM）
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)
set(CMAKE_C_COMPILER /home/wang/ZMHT-Tool/gcc-arm-linux-gnueabi/bin/arm-linux-gnueabihf-gcc)
set(CMAKE_CXX_COMPILER /home/wang/ZMHT-Tool/gcc-arm-linux-gnueabi/bin/arm-linux-gnueabihf-g++)

# 查找路径模式配置（确保交叉编译时正确查找目标系统的库和头文件）

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)    # 不优先在目标系统查找程序
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)     # 只在目标系统查找库
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)     # 只在目标系统查找头文件
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)     # 只在目标系统查找包

# 项目名称为 zynq_can_test（ZYNQ 平台的 CAN 测试
project(zynq_can_test) 

# 定义可执行文件名称为 axican_app
set(TARGETS "tcp_server_can") 

# 设置 C++ 标准为 C++14
set(CMAKE_CXX_STANDARD 14)  
set(CMAKE_CXX_FLAGS "-O2 -std=c++11 -Wno-error")


# 定义宏：
# -D_GNU_SOURCE：启用 GNU 扩展功能
# -DLINUX：标识 Linux 环境
add_definitions(-D_GNU_SOURCE -DLINUX)

#在那个目录下找头文件
include_directories(
    ${CMAKE_CURRENT_LIST_DIR}/src/          # 主源码目录
    ${CMAKE_CURRENT_LIST_DIR}/src/axican/   # CAN 相关源码的头文件目录
)

#那个目录下找链接文件
link_directories(	
    ${CMAKE_CURRENT_LIST_DIR}/lib/          # 自定义库文件目录
)

# 用户态程序源码。不要用 aux_source_directory 自动收集整个 src，
# 因为 src/zmuav_pl2ps_irq.c 是 Linux 内核模块源码，不能编进用户态程序。
set(DIR_SRCS
    src/main18.c
    src/ringbuffer_channels.c
    src/can_frame_queue.c
    src/flashcp.c
)

set(DIR_AXICAN
    src/axican/axican.c
)
#生成可执行编译文件
add_executable(${TARGETS} ${DIR_SRCS} ${DIR_AXICAN})

# 链接需要的系统库：
# - pthread：线程库
# - m：数学库
# - rt：实时库（用于 POSIX 实时扩展）
# - dl：动态链接库（用于动态加载函数）
target_link_libraries(${TARGETS}           
    pthread
    m
    rt
    dl		
)
