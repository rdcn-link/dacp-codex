# EcoFlow operator review

Reviewed against upstream commit `84de10436a0d382e0d8d2ce1ab3a82f71ebafc29`. All 67 derived operators are inventoried. 44 are published; 20 require external execution environments; 3 have no direct callable processing equivalent. Publication is individual, after verification, not a bulk release.

| Upstream class | Repository name | Executor | Inputs → outputs | Execution mode |
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

## Contracts and examples

Each repository manifest and `specs.json` define the exact port names and representations. Parameters retain upstream spelling unless explicitly replaced by streaming inputs/outputs. `params` is a JSON object, not a positional argument string. Use `view: records` for structured dataset inputs and `view: bytes` for file content. Upstream edges use `task` and `port`.

For example, `piflow_select_field@1.0` with `{"columnNames":"station_id,temperature"}` transforms `[{"station_id":"A","temperature":12.5,"quality":0}]` into `[{"station_id":"A","temperature":12.5}]`. `piflow_csv_save@1.0` converts that record stream to UTF-8 bytes `station_id,temperature\nA,12.5\n`. `piflow_csv_parser@1.0` returns string values, so reading the CSV produces temperature `"12.5"`.

`piflow_join@1.0` receives left/right records and parameters `{"joinMode":"inner","correlationColumn":"station_id"}`. Left `[{"station_id":"A","temperature":12.5}]` and right `[{"station_id":"A","height":30}]` produce one joined record containing all three fields. Colliding non-key right columns receive `_right` suffixes. `piflow_route@1.0` with `{"condition":"temperature > 30"}` sends every record to `result`, matches to `matched`, and the remainder, including SQL NULL predicates, to `unmatched`.

SQL adaptations use SQLite rather than Spark SQL. Scalar types and results can differ; unsupported nested values are rejected. Record comparison/join operations require the schemas documented in their tests. Global comparisons, joins, SQL, and scientific algorithms may materialize inputs; consult `mode` and limits. Statistical fixtures are implementation checks, not independent scientific certification.

## Reviewed algorithm corrections

- Batch thresholds: extract the row's field value rather than its bracketed string representation.
- Fixed MDV interpolation: accumulate dropped fields so multiple replacements do not leave duplicate columns. Preserve the upstream mean's zero contribution for flagged samples; `zone` remains an output field.
- Forward/back interpolation: honor the supplied quality-control marker. Two-consecutive-missing interpolation: honor the marker and use signed differences for decreasing series.
- Planar-fit rotation: use distinct Fc/Le/Hs/u-star output indices instead of repeating Fc four times.
- Fixed outlier windows: honor `statisticalType`. Sliding windows: fix malformed SQL and the undefined `contrast_flag` reference. These adapters explicitly accept one field per step; chain steps for multiple fields.
- Sensible heat profiles: honor the supplied density column name. Profile storage adapters require exactly seven layers, matching the original fixed output tuple.
- Reichstein: use twenty non-overlapping wind bins, deterministic candidate ordering, and only valid thresholds in the median. Exclude missing/nonfinite/flagged estimation samples; require at least 120 valid observations. The implementation is Python and does not start Spark.
- EC storage retains the upstream flag/large-jump condition and 1800-second interval. Rainfall screening retains upstream dropping of rows without a non-null matched rainfall value. Day state uses the upstream approximate sunrise equation, Beijing-time hours and actual day-of-year; AddDayID's relative day index is not automatically a day-of-year.

Original files remain unchanged in `upstream/`; Scala build adaptations are explicit in `spark/build.py`, and native rewrites are in `operators/`. Both are included in source archives. Spark runs with two local workers and small shuffle partitions; the EC adapter reduces the original twenty partitions to two. Adaptive queries and automatic broadcast joins are disabled in the current adapter to reduce thread pressure under the 128-thread container limit. Large or complex workloads still require resource testing.

## Not published

| Upstream class | Reason |
|---|---|
| DifferentialFiltering | Its FluxDifference utility returns the input unchanged; no working differential filtering algorithm exists in the reviewed implementation. |
| Trager | Empty perform method, with no output. Workflow dependency edges express execution order. |
| AddCache | Spark DataFrame.cache is local to a SparkSession; isolated DACP operators cannot provide reusable downstream cache semantics through an identity operator. |

The following 20 operators require external services and an execution/network decision. They are not published as placeholders. Current DACP task containers use network=none; endpoint parameters alone cannot enable access. DockerExecute additionally requires explicit Docker daemon access, and MysqlReadIncremental depends on upstream framework substitution/state behavior outside its Stop implementation.

| Category | Operators |
|---|---|
| HDFS | FileDownHdfs, GetHdfs, ListHdfs, PutHdfs, SaveToHdfs, SelectFilesByName |
| Hive | PutHiveMode, SelectHiveQL |
| Databases | ImpalaRead, JdbcReadFromOracle, MysqlRead, MysqlReadIncremental, MysqlWrite, OracleRead, OracleReadByPartition, OracleWrite, PostgresqlRead, PostgresqlWrite |
| Remote scripts | DockerExecute, ExecuteShell |
