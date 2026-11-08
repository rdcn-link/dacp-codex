# DACP 文档

使用指南帮助连接服务、读取数据与运行 Job；运维指南介绍部署、数据发布、安全、监控和维护；开发指南面向应用集成、嵌入服务与协议开发。

[English](README.md)

## 使用指南 · 快速上手

- [使用指南](usage_zh.md)
- [常见问题](faq_zh.md)
- [连接第一个 DACP 服务](quickstart_zh.md)
- [Shell 与 SDK 指南](users/README_zh.md)

## 使用指南 · 远程数据访问

- [节点数据集目录](dataset-directory_zh.md)
- [记录查询：过滤、投影和限制](records-scans_zh.md)
- [S3 兼容数据读取服务](s3-service_zh.md)
- [DACP 与 FTP 传输实测](transfer-benchmark_zh.md)
- [DACP Scala SDK](../dacp-client-scala/README_zh.md)

## 使用指南 · Job 与结果

- [向 DACP 提交流式作业](jobs_zh.md)
- [提交执行策略与请求重放](materialization_zh.md)
- [看板与执行计划](node-web_zh.md)
- [本地与跨节点流式作业执行对比](local-vs-union-benchmark_zh.md)
- [Stream 与逐步物化对比](materialization-benchmark_zh.md)
- [Union 的 DACP 代理服务](union-proxy_zh.md)

## 运维指南 · 部署与安全

- [运维指南](operations/README_zh.md)
- [认证与访问控制](security_zh.md)
- [DACP Union 服务](../dacp-union-server/README_zh.md)
- [两节点气象协作](../examples/union/README_zh.md)
- [双节点流处理示例](../examples/multi-node/README_zh.md)
- [在一个 DACP 节点上提供文件与 SQL 表](../examples/sql/README_zh.md)
- [DACP 代理服务器](../dacp-proxy-server/README_zh.md)
- [DACP 插件接入](plugins_zh.md)
- [DACP 服务参考](reference_zh.md)

## 运维指南 · 监控与维护

- [监控与看板配置](operations/monitoring_zh.md)
- [用户访问日志](access-logs_zh.md)
- [任务执行记录](execution-history_zh.md)

## 开发指南 · 服务集成

- [开发指南](developers/README_zh.md)
- [在用户程序中启动 DACP Server](embedded-server_zh.md)
- [嵌入式 DACP Server 示例](../examples/embedded-server/README_zh.md)
- [独立贡献插件示例](../examples/plugins/README_zh.md)
- [DACP Python 协作 SDK](../dacp-collaboration-client/README_zh.md)
- [DACP Scala 协作 SDK](../dacp-collaboration-client-scala/README_zh.md)

## 开发指南 · 协议与参考

- [DACP 协议](protocol_zh.md)
- [单 Union 协作协议](union_zh.md)
- [Union 传输协议模块](../dacp-union-protocol/README_zh.md)
- [DEBUG 源码算子](operator-source_zh.md)

数据模型、格式、Job 语言与算子开发请阅读 [XStream 文档](http://xstream.conoai.cn/docs.html)。

[数据集发布策略](dataset-publication_zh.md)：公开、不公开、定时公开及 Union 信任边界。
