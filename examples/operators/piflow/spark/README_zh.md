# Scala 科学算子运行环境

EcoFlow 的科学算子保留上游 Scala / Spark 算法，通过私有适配层接入 DACP SDK。不会将 Spark 依赖加入 DACP 核心模块。每个算子分别编译、测试和发布为小型 JAR，共用一个固定镜像摘要的运行环境：JDK 17、Scala 2.13.16、Spark 4.0.1 及其发行包中的依赖。通用 Python 算子继续使用更小的 Python 运行环境。

## 构建

在项目根目录准备 SDK 和 API JAR，并获取固定版本的 Spark 发行内容：

```bash
(cd dacp-plugin-api && sbt package)
(cd dacp-operator-sdk && sbt package)
.venv/bin/python -m pip download --no-deps --dest /tmp/dacp-piflow-deps pyspark==4.0.1
mkdir -p /tmp/dacp-piflow-spark
tar -xzf /tmp/dacp-piflow-deps/pyspark-4.0.1.tar.gz \
  -C /tmp/dacp-piflow-spark --strip-components=1
.venv/bin/python examples/operators/piflow/spark/build-runtime.py \
  --spark-home /tmp/dacp-piflow-spark
.venv/bin/python examples/operators/piflow/spark/publish.py add_day_id \
  --spark-home /tmp/dacp-piflow-spark
```

当前本地辅助脚本使用 `sudo -n -u cnic -g docker` 调用 Docker，以匹配演示机权限；其他机器需按其管理员配置调整这一调用前缀。运行环境由本机 JDK 17 的 `jlink` 和 Spark JAR 构建，不拉取基础镜像。保留 `java.desktop` 中 Spark 需要的 `java.beans` 类，但不包含图形本机库；仅支持无图形计算。发布需访问本地仓库的 token 文件。

## 合同和限制

- 输入为具名 `records` 流，输出为 `result` 的 `records` 流。端口对应关系和参数示例见上级目录的 `specs.json`。
- Spark 适配层完整读取输入后执行，单输入最多 128 MiB 或一百万条记录；这不是常量内存流算法。输出按批次返回，并在收集 Spark 可复用行之前复制行内容，防止记录被覆盖。
- Spark 采用 `local[2]`、UTC 时区、关闭 Web UI、禁用 ANSI 模式以接近上游 SQL 行为。共享运行环境不意味着多个工作流任务自动共享一个 SparkSession。
- 每个算子发布前在与节点相同的禁网、只读根目录、1 GiB 内存、128 进程/线程限额、512 MiB 非可执行临时目录约束下测试。Spark 可能在该临时目录生成执行文件和 shuffle；它们不作为可恢复结果保存。
- 上游原文始终保留；构建时应用可审查的 Scala 2.13 / Spark 4 适配，不直接修改原文。已包含 `StringBuilder.toString.stripSuffix`、稳定的 `spark.implicits` 导入、`Row.fromSeq` 的不可变快照和数学常量 PI 导入修正。
- 运行成功用例不构成科学模型有效性认证；实际分析仍需检查单位、缺失值、参数范围与适用的数据条件。每个算子的具体修复和已验证数值应分别记录。

当前适配关闭自适应查询与自动广播关联，限制 RPC/shuffle 线程，并将 EC 的二十个分区调整为两个，以减少容器线程压力。完整修正和未移植项见 [移植审阅表](../review_zh.md)。Reichstein 使用父目录的 Python 实现，不通过此 Spark 构建器构建。

仓库只接收源码，旧示例中的本地构建仅用于开发验证；公开发布采用[源码提交与审核流程](../../../../docs/operator-submission_zh.md)。
