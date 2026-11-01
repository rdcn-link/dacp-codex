# 服务集成与开发

DACP 基于内嵌的 XStream 引擎发布 Dataset，并提供远程 Job 执行。Shell 与 SDK 用法从[用户指南](../users/README_zh.md)开始。

## 集成应用

- [嵌入服务器](../embedded-server_zh.md)：启动、关闭及注入应用数据目录。
- [Scala 客户端](../../dacp-client-scala/README_zh.md)：远程数据与 Job 调用。
- [Python 协作客户端](../../dacp-collaboration-client/README_zh.md)及 [Scala 协作客户端](../../dacp-collaboration-client-scala/README_zh.md)：节点协作调用。
- [安装服务器插件](../plugins_zh.md)：宿主配置与插件生命周期。

## 协议参考

- [DACP 协议](../protocol_zh.md)：发现、远程读取及调用者能力。
- [服务参考](../reference_zh.md)：descriptor、ticket 及配置。
- [Union 协议](../union_zh.md)：成员信任、子 Job 下发及直连数据流。
- [认证](../security_zh.md)：角色、凭据与授权检查。
- [执行历史](../execution-history_zh.md)：服务记录与管理。

线协议定义在 [dacp.proto](../../proto/dacp.proto)。`dacp-server` 承担服务入口与授权；客户端实现远程调用；`dacp-union-server` 协调成员。

## 数据与执行扩展

Dataset 后端、格式、算子、打包、流契约及执行存储实现的指南见 [XStream 开发文档](http://xstream.conoai.cn/docs.html)。DACP 通过服务器配置集成这些插件。

## 验证修改

在仓库根目录运行：

```bash
(cd dacp-server && sbt test)
.venv/bin/python -m pytest -q dacp-client/tests
(cd dacp-client-scala && sbt test)
(cd dacp-union-server && sbt test)
```

跨节点容器集成使用 `bash scripts/test-collaboration.sh`，需要 Docker 以及脚本规定的证书和运行时。同步维护服务调用者、协议定义、示例及中英文文档。
