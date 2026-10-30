# xstream-data

为 XStream 和 DACP 提供独立于任务引擎、网络服务的类型化数据访问。公共契约位于 `xstream.data`，各实现均为独立的 ServiceLoader 插件。参见[类型化数据流](../docs/data-streams_zh.md)和[插件开发](../docs/plugins_zh.md)。

## 模块

- `xstream-data-core`：DataCatalog → Dataset → Item、DataView/DataStream、选择条件、插件生命周期及编码契约。
- `xstream-data-files`、`xstream-data-catalog-static`、`xstream-data-mounted`：文件/JDBC、配置式及挂载目录。
- `xstream-data-formats-basic`、`xstream-data-parquet`、`xstream-data-excel`、`xstream-data-archives`：已有格式读取及结构浏览。
- `xstream-data-netcdf`：NetCDF 分组、数值变量和有界的步长数组选择；非数值变量可以浏览但不提供数组视图。
- `xstream-data-fits`：HDU、有界图像/数组块和支持的定长二进制表；变长、复数表单元不声明 records 能力。
- `xstream-data-fasta`：包含序列 ID、符号偏移和结束标记的有界序列块；按 ID/范围选择时顺序扫描源文件。
- `xstream-data-scientific-codecs`：供网络适配器使用的第 1 版数组、图像、序列描述和分块编码。

## 构建与使用

```bash
cd xstream-data
sbt compile
```

应用在编译时依赖 core，在运行时加载所需实现。DACP 服务端发行包包含科学格式插件。在 `META-INF/services/xstream.data.FormatHandler` 注册格式，在 `META-INF/services/xstream.data.DataViewCodec` 注册编码器，每行填写实现类全名。外部 JAR 及其依赖可放入既有插件目录，不需要修改核心注册表。

文件 Item 可配置 `format: netcdf`、`format: fits` 或 `format: fasta`。NetCDF/FITS 接受 `chunk_bytes`（8–16777216，默认 65536）；FASTA 接受 `chunk_symbols`（1–1048576，默认 65536）及 `alphabet`（`dna`、`rna`、`protein`、`unknown`）。格式参数放入 Item 的 `options` 映射。

NetCDF 子对象使用分组/变量路径；FITS 使用 `/hdu0`、`/hdu1` 等；FASTA 暴露 `/sequences`。文件根保留原始字节。数值保留原始编码、缩放和缺失值元信息，不隐式展平成 records。NetCDF/FITS 库需要本地随机访问：远程 ByteSource 会显式暂存为临时文件，在访问对象关闭时删除；本地文件不复制。NetCDF-4/HDF5 支持取决于安装的 NetCDF 后端，内置纯 Java 读取器不保证支持。压缩 FITS 图像及不支持的列类型不代表具有科学视图支持。

科学数据网络适配器可通过节点、Union 代理和客户端传输类型化描述及有界数据块。read_item 支持科学视图选择，设计器提供结构与样例预览；下游算子需显式支持对应表示。详见[设计器科学视图](../docs/designer-scientific_zh.md)。

解析器与编码契约测试：`cd ../dacp-server && sbt 'testOnly dacp.DataStreamSpec dacp.ScientificFormatsSpec'`。

## 配置示例

```yaml
datasets:
  science:
    provider: mixed
    items:
      /climate:
        file: /srv/science/climate.nc
        format: netcdf
        options: {chunk_bytes: 65536}
      /sky:
        file: /srv/science/sky.fits
        format: fits
        options: {chunk_bytes: 65536}
      /genome:
        file: /srv/science/genome.fa
        format: fasta
        options: {alphabet: dna, chunk_symbols: 65536}
```
