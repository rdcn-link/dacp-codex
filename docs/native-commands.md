# 原生命令算子

已实现：`executor: command` 将一个 bytes 输入端口连接到程序 stdin，将程序 stdout 包装成一个 bytes 输出端口。程序可使用 C、C++、Rust、Go 编译，也可使用现有命令行工具。程序在独立 Docker 容器内执行，不在服务器宿主机启动。普通模式可执行已发布的命令算子，不支持内联原生源码编译。

```yaml
name: uppercase
version: "1.0"
image: example/native-runtime:1
executor: command
entrypoint: /usr/bin/tr
executor_config:
  args: [a-z, A-Z]
  stdin: source
  stdout: {port: result, representation: bytes, format: text}
inputs:
  source: {representations: [bytes], formats: []}
outputs:
  result: {representations: [bytes], formats: [text]}
```

运行时须包含 `/usr/local/bin/python`、PyArrow、可执行程序及匹配系统和架构的依赖。桥接器复用 Python 运行时，但原生程序自身无需依赖 Python、Arrow 或 DACP。`dacp/operator-python:1.0` 可运行[独立 C 制品示例](../examples/operators/native/README.md)，不保证包含 `tr` 等系统工具；请在派生镜像中预装所需程序。

`entrypoint` 必须为绝对路径。`args` 为字面字符串列表，直接传递参数，不经过 shell、不展开变量或替换作业参数。`args`、`stdin`、`stdout` 均为必填，未知设置会被拒绝。输入和输出各声明一个对应端口，且只能支持 bytes。stdout 必须指定 `port`、`representation: bytes` 和非空 `format`，格式还须符合输出端口约束。格式标签不会触发转换。

若独立上传可执行文件，设置 `artifact: path/to/program` 和 `entrypoint: /operator/program`，文件名须一致。本地制品路径相对 `operator.yaml`。下载文件经过校验并只读挂载；由于内容寻址存储不保存执行权限，桥接器将程序复制到容器私有 `/operator-work`、设置执行权限后启动。该目录是插件请求的 512 MiB 有界可执行 tmpfs，普通 `/tmp` 仍不可执行。共享库由运行时提供，执行时不下载依赖或编译。

桥接器从输入 FIFO 的 Arrow IPC 字节单元读取原始字节、写入 stdin，输入结束后关闭 stdin。同时以最大 64 KiB 的块读取 stdout 并产生 DACP start/data/end 帧。管道和缓冲区有界，保留背压，不收集完整输入输出。stderr 仅用于诊断。程序退出成功且输入传递成功后才产生 end；非零退出或输入传递失败使作业失败，已输出的数据是不完整结果。取消和超时使用现有容器清理机制，终止程序及其子进程。命令应消费完整 stdin；提前关闭 stdin 不作为成功截断机制。主动预览请使用 shell 的预览参数。

首版仅支持单 bytes 输入与单 bytes 输出。尚未实现 records 序列化、多管道、文件名参数、可 seek 临时输入文件和 shell 流水线。复杂情况请使用显式算子包装，不隐式展平结构或多维数据。

将配置映射单独保存为 `command.yaml`，端口声明保存为 `ports.yaml`，即可发布：

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish uppercase@1.0 \
  --root .operator-catalog --image example/native-runtime:1 \
  --executor command --entrypoint /usr/bin/tr \
  --ports ports.yaml --executor-config command.yaml
```

独立制品还需 `--artifact path/to/program`，并将入口改为 `--entrypoint /operator/program`。也可用 `seed --operators DIR --root .operator-catalog` 发布完整定义。仓库将插件配置保存为 `execution.config`，执行节点由插件验证；沿用校验和、共享运行时、下载和离线缓存机制。

shell 仍使用 `run uppercase@1.0 --input source=/reports/orders`，无需客户端修改或专用任务语法。

`CommandExecutor` 通过 `META-INF/services/dacp.OperatorExecutor` 注册，位于 `dacp-operator-executors`。外部插件使用同一扩展点：将 `dacp-plugin-api` 作为 Provided 依赖，打包 service 文件，将插件和私有依赖放入 `plugin_directory` 后重启。`OperatorProgram.configuration` 为插件自有设置，`inputs`、`outputs` 将端口名映射到支持的表示集合。插件自行验证，核心只负责通用元数据传递与资源管理。

验证命令及完整行为详见[英文参考](native-commands_en.md)。真实联调脚本 `scripts/test-native-execution.py` 需要已 stage 的服务器、cc、证书、Docker 权限及 Python 运行时镜像；它启动并停止自己的服务器，默认端口 18817，可用 `DACP_NATIVE_TEST_PORT` 修改。
