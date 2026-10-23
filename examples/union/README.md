# 双节点 Union 协作示例

[English](README_en.md)

A 在本地汇总销量，输出 `{"apple": 6, "pear": 3}`；B 直接拉取该流，与自己的价格表融合，产生 `{"apple": "9.00", "pear": "6.00"}`。客户端只连接 X。两个节点的普通下载前缀均为空，Union 仅获得计算授权。

## 准备

以下准备命令从仓库根目录运行。需要 JDK 17、sbt、OpenSSL、Python、PyArrow 和可访问的 Docker 守护进程。若使用 `.venv`，用 `.venv/bin/python` 替换 `python3`。节点必须能够运行共享 Python 镜像；领域逻辑只在小型 Python 算子源码中。

```bash
bash scripts/certs.sh
python3 examples/union/prepare.py
python3 -m pip install -e ./dacp-client
docker build -f examples/operators/python/Dockerfile -t dacp/operator-python:1.0 .
```

准备脚本使用现有 `server` 证书作为 X 身份、`client` 证书作为 C 身份，生成 `.union-demo/x.yaml` 和两个有效期为 24 小时的审批文件。示例证书仅供本机演示。

分别构建节点和 X：

```bash
cd dacp-server
sbt stage
cd ../dacp-union-server
sbt stage
```

## 启动与邀请

三个终端分别运行，保留对应工作目录：

```bash
# 终端 A，工作目录 dacp-server
target/universal/stage/bin/dacp-server ../examples/union/node-a.yaml
```

```bash
# 终端 B，工作目录 dacp-server
target/universal/stage/bin/dacp-server ../examples/union/node-b.yaml
```

```bash
# 终端 X，工作目录 dacp-union-server
target/universal/stage/bin/dacp-union-server serve ../.union-demo/x.yaml
```

在 `dacp-union-server` 目录邀请两个节点，查看 Union ID：

```bash
target/universal/stage/bin/dacp-union-server invite ../.union-demo/x.yaml dacp://localhost:18861
target/universal/stage/bin/dacp-union-server invite ../.union-demo/x.yaml dacp://localhost:18862
target/universal/stage/bin/dacp-union-server identity ../.union-demo/x.yaml
```

在 `dacp-server` 目录，用返回的 `union_id` 替换 `UNION_ID`，由两个节点各自的管理员批准：

```bash
target/universal/stage/bin/dacp-server union approve ../.union-demo/a UNION_ID ../.union-demo/a-policy.yaml
target/universal/stage/bin/dacp-server union approve ../.union-demo/b UNION_ID ../.union-demo/b-policy.yaml
```

这一步明确授予 X 计算权限；持有同一 CA 的证书本身不够。在 X 目录查询：

```bash
target/universal/stage/bin/dacp-union-server members ../.union-demo/x.yaml
```

## 提交与观察

回到仓库根目录：

```bash
python3 examples/union/client.py
```

客户端提交 [job.yaml](job.yaml)，在标准错误增量打印节点 Task 状态与端口流量事件，标准输出打印最终 JSON。中间流由 B 通过 Arrow Flight mTLS 从 A 拉取，X 只接收 B 的最终流。示例分析算子在 `operators/` 中，不需要修改核心注册表；节点的 `local_operator_library` 指向此目录。Python 算子复用同一个运行时镜像，每个算子源码只有数 KB。为了演示清晰，每个输入限制为 16 MiB，并在算子内汇总；传输层本身仍为有界流。

各服务按 Ctrl-C 关闭。成员审批保存在 `.union-demo` 中；输出不落盘。再次邀请和批准已批准成员没有必要。审批过期后，节点先退出，再由 X 邀请并重新批准；重新开始整个示例时先停止全部服务，再删除 `.union-demo` 并重新执行准备。

[配置、接口和限制](../../dacp-union-server/README.md)；[协议与开发约定](../../docs/union.md)。

## 自动验证

完成两个模块的 `sbt stage` 并准备证书与 Python 镜像后，从仓库根目录运行 `python3 scripts/test-union.py`。脚本选择临时端口和状态目录，启动真实 A、B、X 与 Docker 算子，核对结果、直接传输计数和事件，并自动关闭服务。需要相同的 Docker 访问权限。

## 浏览运行看板

`prepare.py` 为 X 配置了本机 HTTP 看板。启动 X 后打开 `http://127.0.0.1:19880/`，可查看 A、B 的审批、健康和容量；提交示例作业后点击作业 ID，查看两项任务的依赖、状态和端口流量。页面每 2 秒刷新，不会领取客户端的结果流。任务通信仍使用 Flight。远程访问及令牌配置见 [HTTP 运行看板](../../dacp-union-server/README.md#管理-api-与看板)。
