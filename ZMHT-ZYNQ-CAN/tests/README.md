9009 命令流回归测试使用生产代码中的解析函数和连接线程，不打开 CAN 设备。通过本地 socketpair 验证逐位置拆包、粘包、超过接收缓冲区的连续报文、错误同步字、超限长度、错误校验、过短载荷及断开时残留半包。

在仓库根目录执行：

```sh
gcc -D_GNU_SOURCE -fno-common -g -fsanitize=address,undefined -pthread \
  -I ZMHT-ZYNQ-CAN/src -I ZMHT-ZYNQ-CAN/src/axican \
  ZMHT-ZYNQ-CAN/tests/test_command_stream.c \
  ZMHT-ZYNQ-CAN/src/ringbuffer_channels.c \
  ZMHT-ZYNQ-CAN/src/axican/axican.c \
  -o /tmp/zynq-command-stream-test
ASAN_OPTIONS=detect_leaks=0 UBSAN_OPTIONS=halt_on_error=1 /tmp/zynq-command-stream-test
```

`-fno-common` 检查全局变量是否被重复定义。旧链表队列已退出应用构建，测试也不再链接该模块。环境须允许本地 socket 收发；受限沙箱可能返回 `Operation not permitted`。测试不加入 ARM 应用构建，不验证 CAN 硬件行为。当前环境不支持 LeakSanitizer，因此运行命令关闭泄漏检测。

9012 下载流回归测试：将上述编译命令中的 `test_command_stream.c` 替换为 `test_download_stream.c`，输出文件改为 `/tmp/zynq-download-stream-test`，然后执行：

```sh
ASAN_OPTIONS=detect_leaks=0 UBSAN_OPTIONS=halt_on_error=1 timeout 20 /tmp/zynq-download-stream-test
```

覆盖普通 CAN 的全部拆包位置、粘包、最大外层包边界、错位同步、错误包头、超限长度、旧 CAN 内层长度、模拟数据发送长度和通道、秒脉冲、广播组入队，以及两条真实连接替换时的半包隔离。测试保留旧命令分流；不启动 CAN 发送或 PPS 线程。

广播并发回归：将编译命令中的测试源替换为 `test_broadcast_concurrency.c`，输出文件改为 `/tmp/zynq-broadcast_concurrency-test`，执行：

```sh
ASAN_OPTIONS=detect_leaks=0 UBSAN_OPTIONS=halt_on_error=1 timeout 20 /tmp/zynq-broadcast_concurrency-test
```

覆盖满队列、每次唤醒一组、重置关闭接收窗口、旧唤醒不能领取新组、轮询和 A/B 交替，以及六个线程并行进行两路入队/出队、PPS/重置和容量查询（每线程 10000 次循环）。不访问网络或硬件。当前环境运行 ThreadSanitizer 报 `unexpected memory mapping`，因此尚未完成 TSan 检查。

广播重置只清理尚未出队的组；已经取到发送线程本地的组按原行为继续发送。状态锁不覆盖 CAN write 或网络应答。该测试不验证板上 PPS 时序，也不覆盖其他模块的共享状态竞争。

缓冲区边界与分配失败回滚测试：

```sh
gcc -g -fsanitize=address,undefined \
  ZMHT-ZYNQ-CAN/tests/test_ringbuffer_safety.c \
  -o /tmp/zynq-ringbuffer-safety-test
ASAN_OPTIONS=detect_leaks=0 UBSAN_OPTIONS=halt_on_error=1 /tmp/zynq-ringbuffer-safety-test
```

测试直接包含生产缓冲区源码，以替换分配/释放函数进行故障注入。覆盖空集合、销毁后访问、调用方通道数大于实际分配数、容量计算溢出、六通道初始化的全部 13 个分配失败位置、正常满队列顺序读取及 100 次创建/销毁。分配跟踪检查每次回滚的未释放块数为零，无需 LeakSanitizer。不测试已有实例的重复初始化或并发销毁；调用方仍须保证初始化及销毁期间无并发访问。

CAN 信息查询回归：将第一条编译命令中的测试源替换为 `test_can_info_query.c`，输出文件改为 `/tmp/zynq-can-info-test`，运行：

```sh
ASAN_OPTIONS=detect_leaks=0 UBSAN_OPTIONS=halt_on_error=1 timeout 20 /tmp/zynq-can-info-test
```

通过本地 socketpair 调用生产信息查询处理函数，检查六路不同波特率/模式、逐路内层校验和、通道计数，以及配置更改后的再次查询。外层长度字段、字节序和 9009 文本确认保留原行为。本批未调整外层校验范围及全局计数器的并发策略。

版本查询回归同时检查 ARM 版本号和 ARM 程序类型。当前固件固定为普通 CAN 版本，
`0xAC` 查询返回 `0x00000001`。CAN FD 版本需要将源码中的 `ARM_PROGRAM_TYPE`
改为 `ARM_PROGRAM_CANFD` 后单独构建。
