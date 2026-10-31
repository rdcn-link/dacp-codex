# 管理员指南

[English](README.md) · [文档目录](../README_zh.md)

管理员负责部署 DACP 数据节点和 Union Server，配置与独立算子仓库的连接，维护证书、授权、数据发布及执行记录。协议与身份边界见 [DACP 协议](../protocol_zh.md)。

数据集清单来自数据库或业务程序时，使用[动态目录插件](../dataset-catalogs_zh.md)；宿主应用直接运行节点时，参见[嵌入式启动](../embedded-server_zh.md)。

## 单节点部署

服务端要求 JDK 17、sbt 1.10.x；算子执行还需要 Linux 和可访问的 Docker Engine。浏览数据的客户端不需要 Docker。先用 `docker info` 检查服务进程是否有访问权限；调整用户组后需要重新登录。

从项目根目录构建：

```bash
(cd dacp-server && sbt stage)
(cd dacp-union-server && sbt stage)
```

本地演示使用项目证书脚本与示例配置；生产环境使用自己的 CA、主机名和证书管理流程。配置中的相对路径以进程工作目录为基准。

```bash
bash scripts/certs.sh
cd dacp-server
sbt 'run ../examples/server.yaml'
```

## 数据发布与算子

在节点配置中声明 Dataset provider 与参数。内置及外部 provider 使用相同的插件契约；文件目录、SQL 连接等配置见[技术参考](../reference_zh.md)、[Dataset provider](../dataset-providers_zh.md)和[SQL 示例](../../examples/sql/README_zh.md)。数据目录及凭据应按服务账号设置权限。

节点通过 `local_operator_library` 读取本地算子，通过 `remote_operator_library_urls` 搜索远程仓库。远程配置顺序影响查找顺序。共享运行时与算子代码分别发布；执行环境和原生命令配置见[打包说明](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-packaging_zh.md)和[执行器手册](https://github.com/rdcn-link/xstream-codex/blob/main/plugins/xstream-plugin-operator-executors/README_zh.md)。数据节点直接执行源码仅在 DEBUG 模式开放；仓库源码提交是正常运行时独立认证的审核服务，配置见[源码运行时](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-source_zh.md)。

## 证书与授权

普通 DACP 客户端使用 TLS 与 Token，无需客户端证书。Union 使用节点签发、绑定 Union ID 与证书指纹的服务凭据及双向 TLS。节点间任务流按执行计划精确授权。凭据本地创建、轮换、撤销并自动加载。操作说明见[认证与访问控制](../security_zh.md)。

## Union 和仓库

- [Union 手册](../../dacp-union-server/README_zh.md)：配置、服务凭据、状态轮询、作业提交和管理看板。
- [双节点示例](../../examples/union/README_zh.md)：证书、节点、Union 和客户端的完整演示。
- [算子仓库手册](https://github.com/rdcn-link/xstream-operator-repository/blob/main/README_zh.md)：初始化、发布、源码材料、HTTP 服务和下载。

Union 从配置构建唯一的 Union，使用节点签发的凭据连接独立节点并轮询状态。HTTP 管理看板展示任务脚本和执行元数据；公开只读，无需登录或 token。计算操作仍通过受授权保护的 DACP 接口进行。

## 监控、记录与停机

看板展示节点健康、启动时间、运行时间、作业 DAG、状态及流量；无法连接的节点状态可能为未知。节点和 Union 保存执行元数据，结果内容不落为历史结果文件。存储位置、历史查询和重启处理见[执行记录](../execution-history_zh.md)。备份应包含持久配置、成员身份与凭据和执行记录；停止服务后复制可避免跨文件不一致。

使用前台进程的 Ctrl+C 或发送 SIGTERM 停机。重启不会恢复中断的结果流。算子镜像可通过 `docker image ls` 检查；本地运行时配置与仓库制品是不同资源。日志中的 DEBUG 级别与允许源码提交的 DEBUG 运行模式分别配置。

节点默认同时开启只读 HTTP 看板，端口为 DACP 服务端口加 1000；可通过 `web.enabled: false` 关闭。配置和 DAG 展示见[节点 Web 看板](../node-web_zh.md)。

节点默认同时启用 [S3 兼容读取服务](../s3-service_zh.md)，端口为 DACP 服务端口加 2000；它通过 HTTPS 与 SigV4 认证下载 bytes Item，签名密钥独立，主体及 reader/runner 角色与 DACP 共用。可通过 `s3: {enabled: false}` 关闭。

Union 对外提供标准 DACP 目录、Item、Job 与 S3 服务；公开短名称、自动放置及管理权限配置见 [Union 代理服务](../union-proxy_zh.md)。

算子仅源码发布：[提交、隔离构建、审核和可信度](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-submission_zh.md)。管理员指定多个分类，开发者选择已注册环境。

引擎、数据 SDK、Designer、仓库服务和官网各有明确的源码与构建目录。构建关联工具前请参见[项目边界](../ecosystem_zh.md)，它们不是本源码目录下的模块。
