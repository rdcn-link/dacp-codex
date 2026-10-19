# 管理员指南

[English](README_en.md) · [文档目录](../README.md)

管理员负责部署数据节点、Union Server 和算子仓库，维护证书、授权、数据发布及执行记录。协议与身份边界见 [DACP 协议](../protocol.md)。

数据集清单来自数据库或业务程序时，使用[动态目录插件](../dataset-catalogs.md)；宿主应用直接运行节点时，参见[嵌入式启动](../embedded-server.md)。

## 单节点部署

服务端要求 JDK 17、sbt 1.10.x；算子执行还需要 Linux 和可访问的 Docker Engine。浏览数据的客户端不需要 Docker。先用 `docker info` 检查服务进程是否有访问权限；调整用户组后需要重新登录。

从项目根目录构建：

```bash
(cd dacp-server && sbt stage)
(cd dacp-union-server && sbt stage)
(cd dacp-operator-repository && sbt stage)
```

本地演示使用项目证书脚本与示例配置；生产环境使用自己的 CA、主机名和证书管理流程。配置中的相对路径以进程工作目录为基准。

```bash
bash scripts/certs.sh
cd dacp-server
sbt 'run ../examples/server.yaml'
```

## 数据发布与算子

在节点配置中声明 Dataset provider 与参数。内置及外部 provider 使用相同的插件契约；文件目录、SQL 连接等配置见[技术参考](../reference.md)、[Dataset provider](../dataset-providers.md)和[SQL 示例](../../examples/sql/README.md)。数据目录及凭据应按服务账号设置权限。

节点通过 `local_operator_library` 读取本地算子，通过 `remote_operator_library_urls` 搜索远程仓库。远程配置顺序影响查找顺序。共享运行时与算子代码分别发布；执行环境和原生命令配置见[打包说明](../operator-packaging.md)和[执行器手册](../../dacp-operator-executors/README.md)。源码提交仅在 DEBUG 模式开放，配置见[源码运行时](../operator-source.md)。

## 证书与授权

Flight 使用 mTLS，调用方验证服务器，服务器验证客户端。节点参与跨节点调用时需要承担客户端角色。CA 签名不等于业务授权；分别配置节点数据权限、Union 客户端授权及节点对 Union 的计算审批。证书须符合连接主机名和对应 TLS 用途。不要提交私钥或将示例 CA 用于正式部署。

## Union 和仓库

- [Union 手册](../../dacp-union-server/README.md)：配置、邀请、审批、状态轮询、作业提交和公开看板。
- [双节点示例](../../examples/union/README.md)：证书、节点、Union 和客户端的完整演示。
- [算子仓库手册](../../dacp-operator-repository/README.md)：初始化、发布、源码材料、HTTP 服务和下载。

Union 维护一个 Union，主动邀请独立节点并轮询状态。成员加入需要节点审批。HTTP 看板公开只读，展示任务脚本和执行元数据；发布前检查任务参数中是否包含不宜公开的内容。计算操作仍通过受授权保护的 Flight 接口进行。

## 监控、记录与停机

看板展示节点健康、启动时间、运行时间、作业 DAG、状态及流量；无法连接的节点状态可能为未知。节点和 Union 保存执行元数据，结果内容不落为历史结果文件。存储位置、历史查询和重启处理见[执行记录](../execution-history.md)。备份应包含持久配置、成员审批状态和执行记录；停止服务后复制可避免跨文件不一致。

使用前台进程的 Ctrl+C 或发送 SIGTERM 停机。重启不会恢复中断的结果流。算子镜像可通过 `docker image ls` 检查；本地运行时配置与仓库制品是不同资源。日志中的 DEBUG 级别与允许源码提交的 DEBUG 运行模式分别配置。
