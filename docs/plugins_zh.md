# DACP 插件接入

在 DACP 服务器中安装并启用数据或执行插件。插件开发、注册契约及示例见 [XStream 插件指南](https://github.com/rdcn-link/xstream-codex/blob/main/docs/plugins_zh.md)。

## 安装与配置

将插件 JAR 及宿主尚未提供的私有依赖安装到 `plugin_directory`，重启服务器加载。不要重复打包公共 API。数据插件编译依赖 `xstream-data-core`；执行扩展依赖 `xstream-execution-api`，使用 `Provided`。插件通过 JAR 内的 `META-INF/services/` 注册，内置和外部实现使用同一机制。

```yaml
plugin_directory: /opt/dacp/plugins
datasets:
  documents:
    provider: directory
    source:
      root: /srv/data
```

插件在服务器 JVM 中运行，是管理员安装的可信代码；它们不是用户上传的算子，也不受算子容器隔离保护。参数校验归插件，服务授权归 DACP。注册变更需要重启。

## 内嵌宿主与生命周期

`DacpServer.builder().plugins(environment, owned = false)` 注入应用持有的 `PluginEnvironment`，该环境不会随服务器关闭。请求结束时释放访问句柄，服务器关闭时释放自身拥有的资源。

执行历史可配置 `execution_store: {provider: file, settings: {directory: /var/lib/dacp/executions}}`。file 实现位于 XStream 的 `plugins/xstream-plugin-execution-file`。接口、原子性与物化规则由 XStream 定义。

## 服务适配

DACP 将共享目录映射为远程数据访问，并对受保护的操作执行授权。S3 适配复用同一数据访问对象，不要求后端再次注册。仅提供 records 的 Item 可通过公共适配器提供 JSONL bytes，详见 [S3 服务](s3-service_zh.md)。

服务配置见[技术参考](reference_zh.md)，内嵌启动见[内嵌服务器](embedded-server_zh.md)。
