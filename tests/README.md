# 转发回归测试

需要 Python 3 和 C 编译器。测试不修改已安装的转发服务。

```bash
python3 tests/test_forwarding.py
python3 tests/test_linux.py
```

`test_forwarding.py` 提取实际的 C 转发函数，模拟 splice/epoll 系统调用，
并使用 AddressSanitizer 和 UndefinedBehaviorSanitizer 检查小管道、背压、
重复事件注册、搬运预算、信号中断、部分写入、EOF 和连接建立等行为。
这部分测试可在 macOS 或 Linux 运行。

`test_linux.py` 需要 Linux，使用真实 TCP、epoll、splice 和 `/proc` CPU 计数。
它只在测试进程内把管道容量限制为 4 KiB，分别检查双向接收端堵塞时的 CPU、
解除堵塞后的 8 MiB 数据完整性，以及半关闭后的反向响应。
测试监听回环地址的随机端口；请在负载较低的机器运行，避免 CPU 断言受干扰。

两个脚本都可接收另一份 `tcp_pool.c` 的路径，用于比较原版：

```bash
python3 tests/test_forwarding.py /path/to/original/tcp_pool.c
python3 tests/test_linux.py /path/to/original/tcp_pool.c
```

故障注入测试的部分断言约束修复后的调度行为，不能把原版的失败数量等同于独立故障数量。
GitHub Actions 会构建程序并运行上述两组测试。测试源码修复时应自行编译，
仓库 `dist/` 中的既有二进制不包含尚未重新构建的源码修改。
