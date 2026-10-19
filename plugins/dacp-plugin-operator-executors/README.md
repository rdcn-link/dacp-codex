# DACP 算子执行器

[English](README_en.md)

为 `container`、`jvm`、`python` 和 `command` 算子提供启动插件。所有算子都在共享的 Docker 运行环境中执行。JAR 和 Python 包与运行环境分开下载，并以只读方式挂载；算子代码不会加载到服务器 JVM 中。

内置实现通过 `META-INF/services/dacp.OperatorExecutor` 注册，与外部插件使用相同的契约。核心负责容器隔离、超时、取消和清理；插件验证自己的制品和入口点，并提供启动命令。

```bash
(cd plugins/dacp-plugin-operator-executors && sbt test)
```

运行环境布局、算子定义、发布、插件注册、限制与契约测试见[算子打包](../../docs/operator-packaging.md)。

`command` 插件将一个 bytes 输入连接到标准输入，并将标准输出包装为 bytes 输出。它支持运行环境内已安装的可执行程序，以及单独发布的原生制品。参见[原生命令配置、注册与测试](../../docs/native-commands.md)。
