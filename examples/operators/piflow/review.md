# EcoFlow 算子移植审阅表

审阅基线为上游提交 `84de10436a0d382e0d8d2ce1ab3a82f71ebafc29`，清单覆盖全部 67 个派生算子。已发布 44 个，20 个依赖外部执行环境，另外 3 个没有可直接移植的处理实现。每个算子分别验证后立即发布。

| 上游类 | 仓库名称 | 执行器 | 输入 → 输出 | 执行模式 |
|---|---|---|---|---|
| ColumnCheck | `piflow_column_check@1.0` | python | left:records, right:records → result:records | bounded_materialization |
| ConsistencyCheck | `piflow_consistency_check@1.0` | python | left:records, right:records → result:records | bounded_materialization |
| DRCoordinateRotation | `piflow_dr_coordinate_rotation@1.0` | jvm | source:records → result:records | spark_batch |
| PFCoordinateRotation | `piflow_pf_coordinate_rotation@1.0` | jvm | source:records → result:records | spark_batch |
| StorageItemEC | `piflow_storage_item_ec@1.0` | jvm | source:records → result:records | spark_batch |
| StorageItemPM_Hs | `piflow_storage_item_pm__hs@1.0` | jvm | left:records, right:records → result:records | spark_batch |
| StorageItemPM_LE | `piflow_storage_item_pm_le@1.0` | jvm | left:records, right:records → result:records | spark_batch |
| WplCorrection | `piflow_wpl_correction@1.0` | jvm | source:records → result:records | spark_batch |
| RainfallDataCulling | `piflow_rainfall_data_culling@1.0` | jvm | left:records, right:records → result:records | spark_batch |
| ReichsteinCheck | `piflow_reichstein_check@1.0` | python | source:records → result:records | bounded_materialization |
| WildSpotCullingMDV_G | `piflow_wild_spot_culling_mdv_g@1.0` | jvm | source:records → result:records | spark_batch |
| WildSpotCullingMDV_I | `piflow_wild_spot_culling_mdv_i@1.0` | jvm | source:records → result:records | spark_batch |
| CurrentAndPostValueInterpolation | `piflow_current_and_post_value_interpolation@1.0` | jvm | source:records → result:records | spark_batch |
| FrontAndBackValuesAreValid | `piflow_front_and_back_values_are_valid@1.0` | jvm | source:records → result:records | spark_batch |
| MDV_G | `piflow_mdv_g@1.0` | jvm | source:records → result:records | spark_batch |
| MDV_I | `piflow_mdv_i@1.0` | jvm | source:records → result:records | spark_batch |
| AddDayID | `piflow_add_day_id@1.0` | jvm | source:records → result:records | spark_batch |
| AddDayState | `piflow_add_day_state@1.0` | jvm | source:records → result:records | spark_batch |
| BatchFieldThresholdClean | `piflow_batch_field_threshold_clean@1.0` | jvm | left:records, right:records → result:records | spark_batch |
| SingleFieldThresholdClean | `piflow_single_field_threshold_clean@1.0` | jvm | source:records → result:records | spark_batch |
| AddUUIDStop | `piflow_add_uuid_stop@1.0` | python | source:records → result:records | streaming |
| ConvertSchema | `piflow_convert_schema@1.0` | python | source:records → result:records | streaming |
| Distinct | `piflow_distinct@1.0` | python | source:records → result:records | streaming_with_bounded_state |
| DropField | `piflow_drop_field@1.0` | python | source:records → result:records | streaming |
| ExecuteSQLStop | `piflow_execute_sql_stop@1.0` | python | source:records → result:records | bounded_materialization |
| Filter | `piflow_filter@1.0` | python | source:records → result:records | streaming |
| Fork | `piflow_fork@1.0` | python | source:records → left:records, right:records | streaming |
| Join | `piflow_join@1.0` | python | left:records, right:records → result:records | bounded_materialization |
| Merge | `piflow_merge@1.0` | python | left:records, right:records → result:records | streaming |
| MockData | `piflow_mock_data@1.0` | python | source:records → result:records | streaming |
| Route | `piflow_route@1.0` | python | source:records → result:records, matched:records, unmatched:records | streaming |
| SelectField | `piflow_select_field@1.0` | python | source:records → result:records | streaming |
| Subtract | `piflow_subtract@1.0` | python | left:records, right:records → result:records | bounded_materialization |
| CsvParser | `piflow_csv_parser@1.0` | python | source:bytes → result:records | streaming |
| CsvSave | `piflow_csv_save@1.0` | python | source:records → result:bytes | streaming |
| CsvStringParser | `piflow_csv_string_parser@1.0` | python | source:bytes → result:records | streaming |
| ExcelRead | `piflow_excel_read@1.0` | python | source:bytes → result:records | bounded_materialization |
| ExcelWrite | `piflow_excel_write@1.0` | python | source:records → result:bytes | bounded_materialization |
| CustomView | `piflow_custom_view@1.0` | python | source:records → result:bytes | streaming |
| Histogram | `piflow_histogram@1.0` | python | source:records → result:records | bounded_materialization |
| LineChart | `piflow_line_chart@1.0` | python | source:records → result:records | bounded_materialization |
| PieChart | `piflow_pie_chart@1.0` | python | source:records → result:records | bounded_materialization |
| ScatterPlotChart | `piflow_scatter_plot_chart@1.0` | python | source:records → result:records | bounded_materialization |
| TableShow | `piflow_table_show@1.0` | python | source:records → result:records | bounded_materialization |

## 契约与示例

每个 manifest 与 `specs.json` 规定准确的端口名和表示形式。参数保留上游拼写，明确改为流输入输出的部分除外。`params` 是 JSON 对象，不是位置参数串。数据集记录输入使用 `view: records`，文件内容使用 `view: bytes`；步骤之间使用 `task`、`port` 连接。

例如，`piflow_select_field@1.0` 参数为 `{"columnNames":"station_id,temperature"}`，把 `[{"station_id":"A","temperature":12.5,"quality":0}]` 转为 `[{"station_id":"A","temperature":12.5}]`。`piflow_csv_save@1.0` 将记录流转换为 UTF-8 字节 `station_id,temperature\nA,12.5\n`。通过 `piflow_csv_parser@1.0` 读回时，温度值为字符串 `"12.5"`。

`piflow_join@1.0` 接收 left/right 记录，参数为 `{"joinMode":"inner","correlationColumn":"station_id"}`。左侧 `[{"station_id":"A","temperature":12.5}]` 与右侧 `[{"station_id":"A","height":30}]` 产生包含这三个字段的关联记录。右侧重名的非键字段添加 `_right` 后缀。`piflow_route@1.0` 参数 `{"condition":"temperature > 30"}` 会将全部记录输出到 `result`，匹配项输出到 `matched`，其余项（包括谓词为 SQL NULL 的记录）输出到 `unmatched`。

SQL 适配使用 SQLite，而非 Spark SQL；标量类型与结果可能不同，不支持的嵌套值会被拒绝。比较、关联所需的模式约束见对应测试。全局比较、关联、SQL 和科学算法可能物化输入，应查看 `mode` 和资源上限。科学数值样例验证实现行为，不等同于独立的科学模型认证。

## 算法审阅和修正

- 批量阈值：直接读取行内字段值，避免把包含方括号的 Row 字符串用于字段匹配。
- 固定窗口 MDV 插补：累积删除多个被替换字段，避免重名列。保留上游缺测值按零贡献参与均值的行为，输出仍包含 `zone`。
- 前后值插补：使用用户指定的缺测标记。连续两个缺测值插补：使用指定标记，并采用有符号差值支持下降序列。
- 平面拟合旋转：分别取 Fc、Le、Hs、u-star，修复四次重复取 Fc 的错误。
- 固定窗口异常值剔除：使用指定 `statisticalType`。滑动窗口：修复 SQL 语法和未定义 `contrast_flag`。这些适配明确每步处理一个字段，多字段可串联多步。
- 显热廓线：使用指定空气密度列。廓线储存项适配要求恰好七层，与上游固定输出元组一致。
- Reichstein：使用二十个不重叠风速区间、确定的候选顺序，仅对有效阈值求中位数。估计时排除 null、非有限值与缺测标记，要求至少 120 条有效观测。实现使用 Python，不启动 Spark。
- EC 储存项保留上游缺测/大跳变判定及 1800 秒间隔。降雨剔除保留上游丢弃没有匹配有效降雨值的通量记录的行为。昼夜状态沿用上游近似日出日落公式，小时按北京时间，日序号为真实年内日序；AddDayID 的相对日序不能直接当作年内日序。

`upstream/` 保留原文，Scala 修改在 `spark/build.py` 中明确列出，独立实现位于 `operators/`，均随源码包提供。Spark 使用两个本地工作线程和较少 shuffle 分区；EC 适配将原有二十个分区减少为两个。当前适配关闭自适应查询和自动广播关联，以降低 128 线程容器限制下的压力；大规模、复杂负载仍需要资源测试。

## 未发布项

| 上游类 | 原因 |
|---|---|
| DifferentialFiltering | 其 FluxDifference 直接返回输入，审阅版本没有实际执行差分过滤。 |
| Trager | perform 方法为空且没有输出；执行顺序通过工作流依赖边表示。 |
| AddCache | Spark DataFrame.cache 仅存在于 SparkSession 内；独立 DACP 算子不能通过一个原样转发算子提供跨步骤可重用缓存。 |

以下 20 项需要外部服务，以及执行/网络方案的决定，没有发布占位实现。目前 DACP 任务容器使用 network=none，只提供连接参数不能让它访问外部服务。DockerExecute 另外需要明确的 Docker daemon 授权；MysqlReadIncremental 依赖 Stop 实现之外的上游框架变量替换和状态管理。

| 类别 | 算子 |
|---|---|
| HDFS | FileDownHdfs, GetHdfs, ListHdfs, PutHdfs, SaveToHdfs, SelectFilesByName |
| Hive | PutHiveMode, SelectHiveQL |
| 数据库 | ImpalaRead, JdbcReadFromOracle, MysqlRead, MysqlReadIncremental, MysqlWrite, OracleRead, OracleReadByPartition, OracleWrite, PostgresqlRead, PostgresqlWrite |
| 远程脚本 | DockerExecute, ExecuteShell |
