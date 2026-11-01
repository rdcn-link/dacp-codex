# 在 DACP 中使用插件

标准发行包已包含数据读取和执行所需的内置插件；额外插件安装到服务器配置的 `plugin_directory` 后重启加载。

- [服务器插件安装与生命周期](../docs/plugins_zh.md)
- [DACP 集成示例](../examples/plugins/README_zh.md)
- [XStream 插件开发与注册](https://github.com/rdcn-link/xstream-codex/blob/main/docs/plugins_zh.md)
- [XStream 数据实现与依赖](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/README_zh.md)

此源码目录用于保存集成说明；本地插件安装目录使用 `.dacp-plugins/` 等独立路径。
