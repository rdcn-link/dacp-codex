# DACP

<img src="assets/dacp-logo.png" alt="DACP 标志：连接的节点组成 Collaborative 的首字母 C" width="480">

[English](README_en.md)

DACP 是基于 Apache Arrow Flight 的数据协作协议与原型实现。各节点通过统一接口发布数据，客户端按逻辑名称浏览文件、SQL 表和 归档与压缩文件，也可以提交 YAML 任务，让计算在数据所在节点执行。

项目提供 Scala 2.13 服务端、Python 和 Scala 客户端 SDK，以及交互式终端 `dacp-shell`。节点通过 URI 直接寻址，以 mTLS 建立连接，无需中心注册服务。

## 文档

| 读者 | 入口 | 内容 |
|---|---|---|
| 用户与节点管理员 | [用户指南](docs/users/README.md) | 快速开始、shell 示例、SDK、Dataset 配置、查询与流式作业 |
| 算子作者与项目贡献者 | [开发者指南](docs/developers/README.md) | 架构、算子编写、插件契约、协议与测试 |

[文档目录](docs/README.md) · [English documentation](docs/README_en.md)

本地体验请按[快速开始](docs/users/README.md#快速开始)操作。在 dacp-shell 中输入 `help` 查看用法示例，或输入 `help run` 查看算子执行示例。

服务端使用 JDK 17 和 Scala 2.13，客户端包括 Python、Scala 与 dacp-shell。浏览数据无需 Docker；算子在独立 Docker 容器内执行，结果以流返回，不保存结果数据集。本项目尚未发布，不为被替换接口保留兼容层。
