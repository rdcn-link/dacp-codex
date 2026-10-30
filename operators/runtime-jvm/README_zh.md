# 共享 JVM 依赖环境

[English](README.md)

本项目仅整理 JVM 依赖，不含算子实现、入口程序或服务器。Java/Scala 算子独立开发，通过仓库提交源码、构建并审批发布。

在项目根目录，使用 Linux、JDK 17、sbt 和 Docker 构建共享环境：

```bash
scripts/build-jvm-runtime.sh
# 可选：显式指定镜像标签
scripts/build-jvm-runtime.sh dacp/operator-jvm:custom
```

脚本在本目录执行 `sbt stageRuntime`，从 `target/runtime/lib` 复制依赖 JAR，生成裁剪的 JVM 与原生压缩库，再通过 [Dockerfile](Dockerfile) 构建镜像。算子代码单独打包，不包含在此镜像中。构建需要访问 Maven 依赖和本地 Docker Engine。开发者选择运行环境提交源码前，管理员应将生成的不可变运行环境镜像与归档注册到仓库。

SDK 发生变更时，应重建环境后再注册新版本。参见[算子提交](../../docs/operator-submission_zh.md)和[打包说明](../../docs/operator-packaging_zh.md)。
