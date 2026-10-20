# 同进程合并性能测试算子

[English](README_en.md)

本例使用 Scala `ByteStreamOperator` 对 ASCII 文本执行大写或小写转换，按 64 KiB 块读取，不缓存整个 Item。Python 对照算子由测试脚本生成 wheel，包含相同业务逻辑和当前 SDK。

## 执行

在仓库根目录运行：

```bash
(cd examples/operators/stream-benchmark && sbt package)
(cd dacp-server && sbt stage)
.venv/bin/python scripts/benchmark-operator-fusion.py \
  --docker-prefix 'sudo -n -u cnic -g docker --' \
  --rows 1000000 4000000 --repeats 3 \
  --output examples/output/fusion-benchmark
```

需要已有测试 CA/客户端/节点证书（`examples/certs`）、支持 PyArrow 的 Python 环境、可用的 Docker，以及本地 `dacp/operator-jvm:1.1` 和 `dacp/operator-python:1.0` 镜像。能够直接访问 Docker 时省略 `--docker-prefix`。输出目录必须尚不存在。

脚本创建两个临时端口上的独立 DACP 服务，仅 `operator_fusion_enabled` 不同；不修改运行中的演示节点。三个步骤分别执行大写、小写、大写转换。数据每行 64 字节，100 万行是 64,000,000 字节，400 万行是 256,000,000 字节。这里测试的是 CSV 文本字节流，行数由换行符统计，并非 Arrow record stream。

原始输入文件预先落盘，作为数据集来源。算子中间结果通过内存管道（合并）或内存队列、Flight 和 FIFO（独立）传递，不保存成普通结果文件。客户端边消费边校验，不保存最终结果；代码包、参数、日志和执行历史仍使用文件。

每种语言与策略先预热一次，然后每个规模重复三次并交替先后顺序。耗时从客户端提交 Job 到读取完整输出流结束，包含 TLS/Flight、算子解析、容器启动和清理、转换及客户端 SHA-256/行数校验，不含服务启动、数据生成及预热。镜像均已在本地，文件系统缓存不主动清空。

输出目录保留 `results.json`、逐次 Job 脚本、配置、访问日志及执行历史。脚本校验输出 SHA-256、字节数、行数、实际执行策略，以及合并策略一个容器/独立策略三个容器。测试结束自动停止测试服务。既有容器限制为每个容器 1 GiB；合并的三个步骤共享这一限制，独立执行则每步各有 1 GiB。没有额外设置 CPU 配额，其他宿主机负载会影响结果。

该测试针对三个轻量流式转换步骤；不能将结果直接推广为计算密集型算子或跨节点网络的加速比例。Python 线程还受到 GIL 与具体扩展库行为的影响。
