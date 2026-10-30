# Scala 常用算子集

[English](README.md)

100 个 Scala 算子：50 个文本、20 个 JSON、20 个 CSV、10 个二进制算子，名称以 `scala_` 开头，版本为 `1.0`。全部使用 `jvm` 执行器、一个共享薄 JAR（内含小型 SDK，Arrow、Jackson、Scala 依赖由运行环境提供） 和 `dacp/operator-jvm:1.1` 运行环境。每个算子有独立 JVM 入口和仓库清单，源码可下载。不使用 Python 执行逻辑；Python 构建脚本只生成元数据和发布材料。

每个算子输入 `source`、输出 `result`，均为 `bytes`。文本、CSV 和 JSON 使用 UTF-8。参数表给出必填参数示例，无参数时传 `{}`；未知或缺少参数会报错。输入以 Arrow 流消费至 EOS，完整 Item 在内存中转换，默认限制 16 MiB；`max_input_bytes` 可设置为 1 至 16777216 的整数，超限失败，不截断。输出通过 DACP 结果流发送，不保存服务器结果文件。这些是有大小上限的文档算子，不适用于无限流；逐批记录处理可使用 [Python 记录算子](../common/README_zh.md)。

CSV 要求唯一表头及一致的行宽，支持引号、转义引号、单元格内换行与 CRLF，输出使用 LF；CSV 转 JSON 保留字符串类型。列重命名和新增列应使用未占用名称。JSON 数组排序支持数值或字符串；字段缺失、类型错误或 UTF-8 无效时失败。`text_fold_german` 仅转小写并展开 ß；`text_html_unescape_basic` 仅还原 `&lt;`、`&gt;`、`&quot;`、`&#x27;`、`&amp;` 五种实体。字符反转和计数以 Unicode 码点为单位，不以字形簇为单位。摘要和计数的文本输出以 LF 结尾。

## 构建与逐个发布

需要 JDK 17、sbt、Python 3 和 PyArrow。先在项目根目录发布本地依赖（按顺序执行，不要并行运行共享构建）。通过 `--runtime-descriptor runtime.json` 提供已注册 JVM 运行环境的描述（已审批 JVM 算子的 `runtime` 对象），并通过 `--runtime-catalog` 指定其 blob 存储；归档摘要会被校验。

```bash
(cd dacp-plugin-api && sbt publishLocal)
(cd dacp-operator-sdk && sbt publishLocal)
.venv/bin/python examples/operators/common-scala/build.py --runtime-descriptor runtime.json
```

构建脚本生成独立入口，编译薄 JAR，并执行 100 个输入输出示例校验；结果在 `examples/operators/common-scala/target/catalog`。发布复用代码、运行环境和源码 blob，通过带令牌的 HTTP API 逐个发布，一条成功即可在网站看到。版本不可覆盖，变更代码应使用新版本。重复发布只接受完全相同的清单。远程发布应使用 HTTPS。下载的源码归档提供实现、构建文件、入口、测试样例与中英文说明；它依赖项目中的 SDK 及构建辅助程序，按本文命令从项目根目录构建。

在现有工作流中引用 `operator: scala_text_uppercase@1.0`，输入绑定到 `source`，输出依赖使用 `port: result`；例如 `scala_text_replace@1.0` 使用 `params: {old: ",", new: ";"}`。可参照 [完整 Union 工作流](../../union/job.yaml) 和 [算子编写指南](../../../docs/operator-authoring_zh.md)。

## 参数及输入输出示例

引号表示示例字符串而非输入本身，`\n` 表示换行，二进制值使用 `hex:`。

| 算子 | 用途 | 参数 | 输入 | 输出 |
|---|---|---|---|---|
| `scala_text_uppercase@1.0` | Unicode 转大写 | `{}` | `"Hello 世界"` | `"HELLO 世界"` |
| `scala_text_lowercase@1.0` | Unicode 转小写 | `{}` | `"Hello"` | `"hello"` |
| `scala_text_titlecase@1.0` | 单词首字母大写 | `{}` | `"hello world"` | `"Hello World"` |
| `scala_text_capitalize@1.0` | 句首大写 | `{}` | `"hELLO"` | `"Hello"` |
| `scala_text_swapcase@1.0` | 大小写互换 | `{}` | `"AbC"` | `"aBc"` |
| `scala_text_strip@1.0` | 去除首尾空白 | `{}` | `" a \n"` | `"a"` |
| `scala_text_lstrip@1.0` | 去除开头空白 | `{}` | `" a "` | `"a "` |
| `scala_text_rstrip@1.0` | 去除结尾空白 | `{}` | `" a "` | `" a"` |
| `scala_text_normalize_spaces@1.0` | 合并连续空白 | `{}` | `"a  b\nc"` | `"a b c"` |
| `scala_text_normalize_newlines@1.0` | 统一换行符 | `{}` | `"a\r\nb\r"` | `"a\nb\n"` |
| `scala_text_remove_blank_lines@1.0` | 删除空白行 | `{}` | `"a\n \nb"` | `"a\nb"` |
| `scala_text_trim_lines@1.0` | 去除各行首尾空白 | `{}` | `" a \n b "` | `"a\nb"` |
| `scala_text_sort_lines@1.0` | 按字典顺序排列各行 | `{}` | `"b\na"` | `"a\nb"` |
| `scala_text_reverse_lines@1.0` | 反转行顺序 | `{}` | `"a\nb"` | `"b\na"` |
| `scala_text_unique_lines@1.0` | 保序去重行 | `{}` | `"a\nb\na"` | `"a\nb"` |
| `scala_text_number_lines@1.0` | 从一开始添加行号 | `{}` | `"a\nb"` | `"1\ta\n2\tb"` |
| `scala_text_head_lines@1.0` | 提取前 n 行 | `{"n":2}` | `"a\nb\nc"` | `"a\nb"` |
| `scala_text_tail_lines@1.0` | 提取后 n 行 | `{"n":2}` | `"a\nb\nc"` | `"b\nc"` |
| `scala_text_filter_lines@1.0` | 保留包含指定文本的行 | `{"text":"app"}` | `"apple\npear"` | `"apple"` |
| `scala_text_exclude_lines@1.0` | 排除包含指定文本的行 | `{"text":"app"}` | `"apple\npear"` | `"pear"` |
| `scala_text_replace@1.0` | 替换文本片段 | `{"old":",","new":";"}` | `"a,b"` | `"a;b"` |
| `scala_text_prefix_lines@1.0` | 各行添加前缀 | `{"prefix":"> "}` | `"a\nb"` | `"> a\n> b"` |
| `scala_text_suffix_lines@1.0` | 各行添加后缀 | `{"suffix":"!"}` | `"a\nb"` | `"a!\nb!"` |
| `scala_text_normalize_nfc@1.0` | Unicode NFC 规范化 | `{}` | `"é"` | `"é"` |
| `scala_text_normalize_nfkc@1.0` | Unicode NFKC 规范化 | `{}` | `"ＡＢ"` | `"AB"` |
| `scala_text_html_escape@1.0` | HTML 文本转义 | `{}` | `"<a>"` | `"&lt;a&gt;"` |
| `scala_text_url_encode@1.0` | URL 百分号编码 | `{}` | `"a b/"` | `"a%20b%2F"` |
| `scala_text_url_decode@1.0` | URL 百分号解码 | `{}` | `"a%20b"` | `"a b"` |
| `scala_json_identity@1.0` | 校验并压缩 JSON | `{}` | `"{\"a\":1}"` | `"{\"a\":1}"` |
| `scala_json_pretty@1.0` | JSON 缩进排版 | `{}` | `"{\"a\":1}"` | `"{\n  \"a\" : 1\n}"` |
| `scala_json_sort_keys@1.0` | 递归排序对象键 | `{}` | `"{\"b\":2,\"a\":1}"` | `"{\"a\":1,\"b\":2}"` |
| `scala_json_keys@1.0` | 列出对象键 | `{}` | `"{\"a\":1}"` | `"[\"a\"]"` |
| `scala_json_values@1.0` | 列出对象值 | `{}` | `"{\"a\":1}"` | `"[1]"` |
| `scala_json_entries@1.0` | 对象转键值对 | `{}` | `"{\"a\":1}"` | `"[[\"a\",1]]"` |
| `scala_json_get@1.0` | 提取顶层键值 | `{"key":"a"}` | `"{\"a\":1}"` | `"1"` |
| `scala_json_select@1.0` | 选择对象字段 | `{"keys":["a"]}` | `"{\"a\":1,\"b\":2}"` | `"{\"a\":1}"` |
| `scala_json_drop@1.0` | 删除对象字段 | `{"keys":["a"]}` | `"{\"a\":1,\"b\":2}"` | `"{\"b\":2}"` |
| `scala_json_rename@1.0` | 重命名对象字段 | `{"old":"a","new":"b"}` | `"{\"a\":1}"` | `"{\"b\":1}"` |
| `scala_json_set@1.0` | 设置对象字段 | `{"key":"b","value":2}` | `"{\"a\":1}"` | `"{\"a\":1,\"b\":2}"` |
| `scala_json_array_length@1.0` | 统计数组长度 | `{}` | `"[1,2]"` | `"2"` |
| `scala_json_array_reverse@1.0` | 反转数组 | `{}` | `"[1,2]"` | `"[2,1]"` |
| `scala_json_array_sort@1.0` | 排序同类数组元素 | `{}` | `"[2,1]"` | `"[1,2]"` |
| `scala_json_array_unique@1.0` | 保序去重 JSON 数组 | `{}` | `"[1,1,2]"` | `"[1,2]"` |
| `scala_json_array_head@1.0` | 提取数组前 n 项 | `{"n":2}` | `"[1,2,3]"` | `"[1,2]"` |
| `scala_json_array_flatten@1.0` | 展平一层数组 | `{}` | `"[[1],[2,3]]"` | `"[1,2,3]"` |
| `scala_json_pluck@1.0` | 提取对象数组的字段 | `{"key":"a"}` | `"[{\"a\":1},{\"a\":2}]"` | `"[1,2]"` |
| `scala_json_filter_equal@1.0` | 按字段相等筛选对象数组 | `{"key":"a","value":2}` | `"[{\"a\":1},{\"a\":2}]"` | `"[{\"a\":2}]"` |
| `scala_json_to_ndjson@1.0` | JSON 数组转 NDJSON | `{}` | `"[{\"a\":1},{\"a\":2}]"` | `"{\"a\":1}\n{\"a\":2}\n"` |
| `scala_csv_identity@1.0` | 校验并规范 CSV | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `scala_csv_select@1.0` | 选择 CSV 列 | `{"columns":["name"]}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name\nb\na\nb\n"` |
| `scala_csv_drop@1.0` | 删除 CSV 列 | `{"columns":["name"]}` | `"name,value\nb,2\na,1\nb,2\n"` | `"value\n2\n1\n2\n"` |
| `scala_csv_rename@1.0` | 重命名 CSV 列 | `{"old":"name","new":"label"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"label,value\nb,2\na,1\nb,2\n"` |
| `scala_csv_head@1.0` | 提取前 n 条 CSV 记录 | `{"n":1}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\n"` |
| `scala_csv_tail@1.0` | 提取后 n 条 CSV 记录 | `{"n":1}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\n"` |
| `scala_csv_reverse@1.0` | 反转 CSV 记录顺序 | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `scala_csv_unique@1.0` | 保序去重 CSV 记录 | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\n"` |
| `scala_csv_sort@1.0` | 按文本列排序 | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\nb,2\nb,2\n"` |
| `scala_csv_sort_numeric@1.0` | 按数值列排序 | `{"column":"value"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\nb,2\nb,2\n"` |
| `scala_csv_filter_equal@1.0` | 保留字段匹配的 CSV 记录 | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\n"` |
| `scala_csv_filter_not_equal@1.0` | 排除字段匹配的 CSV 记录 | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\nb,2\n"` |
| `scala_csv_filter_contains@1.0` | 按子串筛选 CSV 记录 | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\n"` |
| `scala_csv_trim@1.0` | 去除 CSV 单元格首尾空白 | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `scala_csv_uppercase@1.0` | CSV 指定列转大写 | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nB,2\nA,1\nB,2\n"` |
| `scala_csv_lowercase@1.0` | CSV 指定列转小写 | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `scala_csv_fill_empty@1.0` | 填充 CSV 空单元格 | `{"value":"NA"}` | `"name,value\na,\n"` | `"name,value\na,NA\n"` |
| `scala_csv_add_constant@1.0` | 添加常量 CSV 列 | `{"column":"region","value":"east"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value,region\nb,2,east\na,1,east\nb,2,east\n"` |
| `scala_csv_to_ndjson@1.0` | CSV 转 NDJSON 对象 | `{}` | `"a,b\n1,2\n"` | `"{\"a\":\"1\",\"b\":\"2\"}\n"` |
| `scala_csv_count@1.0` | 统计 CSV 数据行数 | `{}` | `"a\nx\ny\n"` | `"{\"rows\":2}"` |
| `scala_bytes_sha256@1.0` | 计算数据摘要 | `{}` | `"abc"` | `"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad\n"` |
| `scala_bytes_sha512@1.0` | 计算数据摘要 | `{}` | `"abc"` | `"ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f\n"` |
| `scala_bytes_sha3_256@1.0` | 计算数据摘要 | `{}` | `"abc"` | `"3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532\n"` |
| `scala_bytes_base64_encode@1.0` | Base64 编码 | `{}` | `"abc"` | `"YWJj"` |
| `scala_bytes_base64_decode@1.0` | 严格 Base64 解码 | `{}` | `"YWJj"` | `"abc"` |
| `scala_bytes_hex_encode@1.0` | 十六进制编码 | `{}` | `"abc"` | `"616263"` |
| `scala_bytes_hex_decode@1.0` | 十六进制解码 | `{}` | `"616263"` | `"abc"` |
| `scala_bytes_count@1.0` | 统计输入字节数 | `{}` | `"abc"` | `"{\"bytes\":3}"` |
| `scala_text_fold_german@1.0` | 转小写并展开德语尖音 s | `{}` | `"Straße"` | `"strasse"` |
| `scala_text_html_unescape_basic@1.0` | 解码五种基本 HTML 实体 | `{}` | `"&amp;"` | `"&"` |
| `scala_bytes_sha384@1.0` | SHA-384 摘要 | `{}` | `"abc"` | `"cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7\n"` |
| `scala_bytes_crc32@1.0` | CRC32 十进制校验和 | `{}` | `"abc"` | `"891568578\n"` |
| `scala_text_reverse_characters@1.0` | 反转 Unicode 码点 | `{}` | `"ab世界"` | `"界世ba"` |
| `scala_text_count_characters@1.0` | 统计 Unicode 码点 | `{}` | `"ab世界"` | `"4\n"` |
| `scala_text_count_lines@1.0` | 统计行数 | `{}` | `"a\nb\n"` | `"2\n"` |
| `scala_text_count_words@1.0` | 按空白统计词数 | `{}` | `"a b\nc"` | `"3\n"` |
| `scala_text_remove_digits@1.0` | 删除 ASCII 数字 | `{}` | `"a1b2"` | `"ab"` |
| `scala_text_keep_digits@1.0` | 仅保留 ASCII 数字 | `{}` | `"a1b2"` | `"12"` |
| `scala_text_remove_ascii_punctuation@1.0` | 删除 ASCII 标点 | `{}` | `"a,b!"` | `"ab"` |
| `scala_text_tabs_to_spaces@1.0` | 制表符转四个空格 | `{}` | `"a\tb"` | `"a    b"` |
| `scala_text_spaces_to_tabs@1.0` | 四个空格转制表符 | `{}` | `"a    b"` | `"a\tb"` |
| `scala_text_join_lines@1.0` | 用分隔符连接各行 | `{"separator":","}` | `"a\nb"` | `"a,b"` |
| `scala_text_split_words@1.0` | 按空白分词并分行 | `{}` | `"a b\tc"` | `"a\nb\nc"` |
| `scala_text_sort_lines_desc@1.0` | 按字典倒序排列各行 | `{}` | `"a\nb"` | `"b\na"` |
| `scala_text_nonempty_count@1.0` | 统计非空白行 | `{}` | `"a\n \nb"` | `"2\n"` |
| `scala_text_remove_bom@1.0` | 去除开头的 Unicode BOM | `{}` | `"﻿abc"` | `"abc"` |
| `scala_text_ensure_newline@1.0` | 确保末尾有 LF | `{}` | `"abc"` | `"abc\n"` |
| `scala_text_remove_newlines@1.0` | 删除回车换行 | `{}` | `"a\r\nb"` | `"ab"` |
| `scala_text_ascii_only@1.0` | 只保留 ASCII 字符 | `{}` | `"abc世界"` | `"abc"` |
| `scala_text_normalize_nfd@1.0` | Unicode NFD 规范化 | `{}` | `"é"` | `"é"` |
| `scala_text_normalize_nfkd@1.0` | Unicode NFKD 规范化 | `{}` | `"Ａé"` | `"Aé"` |
| `scala_text_strip_accents@1.0` | 删除 Unicode 组合附加符 | `{}` | `"café"` | `"cafe"` |

仓库只接收源码，旧示例中的本地构建仅用于开发验证；公开发布采用[源码提交与审核流程](../../../docs/operator-submission_zh.md)。
