# 常用算子集

[English](README_en.md)

提供 100 个独立命名的算子：30 个文本、20 个 JSON、20 个 CSV、20 个 Arrow 记录、10 个二进制算子。执行器为 `python`，共享 `dacp/operator-python:1.0` 环境和一个小型 wheel；每个算子有独立入口，发布版本由 `--version` 指定（默认 `1.1`），不可覆盖和可下载源码，不需要 100 个容器镜像。仓库本身不执行算子；运行时由 DACP 节点启动。

所有算子只读 `source`、写 `result`。除 `records_*` 外，输入输出均为 `bytes`；文本与 CSV 使用 UTF-8，JSON 输出为 UTF-8。`records_*` 使用 Arrow `records`，逐批变换并保留记录结构；过滤、列选择和计算均逐批进行，没有跨批排序或聚合。支持输入空批；输入必须完整消费至 EOS。下表列出的参数是必填项；无参数算子使用 `{}`，未知参数会报错。

文档类变换需要完整的单个 Item，按 Arrow 流读取后在内存中处理，默认最大 16 MiB。可用 `max_input_bytes` 降低限制（1 至 16777216），超限报错，不截断。这些算子适合小型文档，不应当作无限流转换器。记录算子逐批处理，不受该文档大小限制。输出依然通过 DACP 流传送，不在服务器保存结果文件。

CSV 必须包含唯一列名的表头，每行宽度须一致；输出换行符为 LF。CSV JSON 转换保留单元格为字符串。JSON 数组排序要求元素可比较；字段不存在、UTF-8 无效或类型不匹配会失败。列重命名及新增列应使用未占用的目标名称，避免产生重名字段。GZIP 输出无时间戳，方便内容校验。

## 构建、验证和发布

在项目根目录运行，先准备现有 Python 运行环境的仓库清单 `identity_python@1.1`。构建仅复用该清单的固定镜像摘要和已验证归档，不需要重新导出镜像。运行测试需要 PyArrow。

```bash
.venv/bin/python scripts/test-common-operators.py
.venv/bin/python examples/operators/common/build.py --runtime-catalog .operator-catalog
export DACP_REPOSITORY_PUBLISH_TOKEN="$(cat .operator-catalog/.publish-token)"
.venv/bin/python examples/operators/common/build.py --runtime-catalog .operator-catalog --publish http://127.0.0.1:8820
```

构建结果位于 `examples/operators/common/target/catalog`。HTTP 发布复用共享 blob，并逐个提交算子清单；每成功一个即可在网站浏览。重复运行验证同名同版本内容完全相同，不覆盖既有版本；修改代码应更新版本。令牌只用于接口，不在网站展示。远程发布使用 HTTPS。源码归档包含算子实现、入口、SDK 及中英文说明。没有添加未经核实的许可证声明。

## 工作流示例

在已有任务模板中使用下列步骤（`node` 和 `uri` 替换为实际节点与 Item），然后通过 Union 或节点客户端提交。`params` 的内容也可用于 shell `run` 的参数对象；完整命令参见 [算子编写指南](../../../docs/operator-authoring.md)。

```yaml
id: clean_text
title: 文本转大写
node: dacp://localhost:18861
operator: text_uppercase@1.1
inputs:
  source:
    uri: dacp://localhost:18861/datasets/example/items/message
params: {}
```

## 算子、参数与输入输出示例

以下二进制值用 `hex:` 表示，记录值用列名到数组的 JSON 对象表示。文本的 `\n` 表示换行；表格里的引号是示例表示法，不属于原始字节流。

| 算子 | 用途 | 参数 | 输入 | 输出 |
|---|---|---|---|---|
| `text_uppercase@1.1` | Unicode 转大写 | `{}` | `"Hello 世界"` | `"HELLO 世界"` |
| `text_lowercase@1.1` | Unicode 转小写 | `{}` | `"Hello"` | `"hello"` |
| `text_casefold@1.1` | Unicode 无大小写规范化 | `{}` | `"Straße"` | `"strasse"` |
| `text_titlecase@1.1` | 单词首字母大写 | `{}` | `"hello world"` | `"Hello World"` |
| `text_capitalize@1.1` | 句首大写 | `{}` | `"hELLO"` | `"Hello"` |
| `text_swapcase@1.1` | 大小写互换 | `{}` | `"AbC"` | `"aBc"` |
| `text_strip@1.1` | 去除首尾空白 | `{}` | `" a \n"` | `"a"` |
| `text_lstrip@1.1` | 去除开头空白 | `{}` | `" a "` | `"a "` |
| `text_rstrip@1.1` | 去除结尾空白 | `{}` | `" a "` | `" a"` |
| `text_normalize_spaces@1.1` | 合并连续空白 | `{}` | `"a  b\nc"` | `"a b c"` |
| `text_normalize_newlines@1.1` | 统一换行符 | `{}` | `"a\r\nb\r"` | `"a\nb\n"` |
| `text_remove_blank_lines@1.1` | 删除空白行 | `{}` | `"a\n \nb"` | `"a\nb"` |
| `text_trim_lines@1.1` | 去除各行首尾空白 | `{}` | `" a \n b "` | `"a\nb"` |
| `text_sort_lines@1.1` | 按字典顺序排列各行 | `{}` | `"b\na"` | `"a\nb"` |
| `text_reverse_lines@1.1` | 反转行顺序 | `{}` | `"a\nb"` | `"b\na"` |
| `text_unique_lines@1.1` | 保序去重行 | `{}` | `"a\nb\na"` | `"a\nb"` |
| `text_number_lines@1.1` | 从一开始添加行号 | `{}` | `"a\nb"` | `"1\ta\n2\tb"` |
| `text_head_lines@1.1` | 提取前 n 行 | `{"n":2}` | `"a\nb\nc"` | `"a\nb"` |
| `text_tail_lines@1.1` | 提取后 n 行 | `{"n":2}` | `"a\nb\nc"` | `"b\nc"` |
| `text_filter_lines@1.1` | 保留包含指定文本的行 | `{"text":"app"}` | `"apple\npear"` | `"apple"` |
| `text_exclude_lines@1.1` | 排除包含指定文本的行 | `{"text":"app"}` | `"apple\npear"` | `"pear"` |
| `text_replace@1.1` | 替换文本片段 | `{"old":",","new":";"}` | `"a,b"` | `"a;b"` |
| `text_prefix_lines@1.1` | 各行添加前缀 | `{"prefix":"> "}` | `"a\nb"` | `"> a\n> b"` |
| `text_suffix_lines@1.1` | 各行添加后缀 | `{"suffix":"!"}` | `"a\nb"` | `"a!\nb!"` |
| `text_normalize_nfc@1.1` | Unicode NFC 规范化 | `{}` | `"é"` | `"é"` |
| `text_normalize_nfkc@1.1` | Unicode NFKC 规范化 | `{}` | `"ＡＢ"` | `"AB"` |
| `text_html_escape@1.1` | HTML 文本转义 | `{}` | `"<a>"` | `"&lt;a&gt;"` |
| `text_html_unescape@1.1` | 还原 HTML 实体 | `{}` | `"&amp;"` | `"&"` |
| `text_url_encode@1.1` | URL 百分号编码 | `{}` | `"a b/"` | `"a%20b%2F"` |
| `text_url_decode@1.1` | URL 百分号解码 | `{}` | `"a%20b"` | `"a b"` |
| `json_identity@1.1` | 校验并压缩 JSON | `{}` | `"{\"a\":1}"` | `"{\"a\":1}"` |
| `json_pretty@1.1` | JSON 缩进排版 | `{}` | `"{\"a\":1}"` | `"{\n  \"a\": 1\n}"` |
| `json_sort_keys@1.1` | 递归排序对象键 | `{}` | `"{\"b\":2,\"a\":1}"` | `"{\"a\":1,\"b\":2}"` |
| `json_keys@1.1` | 列出对象键 | `{}` | `"{\"a\":1}"` | `"[\"a\"]"` |
| `json_values@1.1` | 列出对象值 | `{}` | `"{\"a\":1}"` | `"[1]"` |
| `json_entries@1.1` | 对象转键值对 | `{}` | `"{\"a\":1}"` | `"[[\"a\",1]]"` |
| `json_get@1.1` | 提取顶层键值 | `{"key":"a"}` | `"{\"a\":1}"` | `"1"` |
| `json_select@1.1` | 选择对象字段 | `{"keys":["a"]}` | `"{\"a\":1,\"b\":2}"` | `"{\"a\":1}"` |
| `json_drop@1.1` | 删除对象字段 | `{"keys":["a"]}` | `"{\"a\":1,\"b\":2}"` | `"{\"b\":2}"` |
| `json_rename@1.1` | 重命名对象字段 | `{"old":"a","new":"b"}` | `"{\"a\":1}"` | `"{\"b\":1}"` |
| `json_set@1.1` | 设置对象字段 | `{"key":"b","value":2}` | `"{\"a\":1}"` | `"{\"a\":1,\"b\":2}"` |
| `json_array_length@1.1` | 统计数组长度 | `{}` | `"[1,2]"` | `"2"` |
| `json_array_reverse@1.1` | 反转数组 | `{}` | `"[1,2]"` | `"[2,1]"` |
| `json_array_sort@1.1` | 排序同类数组元素 | `{}` | `"[2,1]"` | `"[1,2]"` |
| `json_array_unique@1.1` | 保序去重 JSON 数组 | `{}` | `"[1,1,2]"` | `"[1,2]"` |
| `json_array_head@1.1` | 提取数组前 n 项 | `{"n":2}` | `"[1,2,3]"` | `"[1,2]"` |
| `json_array_flatten@1.1` | 展平一层数组 | `{}` | `"[[1],[2,3]]"` | `"[1,2,3]"` |
| `json_pluck@1.1` | 提取对象数组的字段 | `{"key":"a"}` | `"[{\"a\":1},{\"a\":2}]"` | `"[1,2]"` |
| `json_filter_equal@1.1` | 按字段相等筛选对象数组 | `{"key":"a","value":2}` | `"[{\"a\":1},{\"a\":2}]"` | `"[{\"a\":2}]"` |
| `json_to_ndjson@1.1` | JSON 数组转 NDJSON | `{}` | `"[{\"a\":1},{\"a\":2}]"` | `"{\"a\":1}\n{\"a\":2}\n"` |
| `csv_identity@1.1` | 校验并规范 CSV | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `csv_select@1.1` | 选择 CSV 列 | `{"columns":["name"]}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name\nb\na\nb\n"` |
| `csv_drop@1.1` | 删除 CSV 列 | `{"columns":["name"]}` | `"name,value\nb,2\na,1\nb,2\n"` | `"value\n2\n1\n2\n"` |
| `csv_rename@1.1` | 重命名 CSV 列 | `{"old":"name","new":"label"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"label,value\nb,2\na,1\nb,2\n"` |
| `csv_head@1.1` | 提取前 n 条 CSV 记录 | `{"n":1}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\n"` |
| `csv_tail@1.1` | 提取后 n 条 CSV 记录 | `{"n":1}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\n"` |
| `csv_reverse@1.1` | 反转 CSV 记录顺序 | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `csv_unique@1.1` | 保序去重 CSV 记录 | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\n"` |
| `csv_sort@1.1` | 按文本列排序 | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\nb,2\nb,2\n"` |
| `csv_sort_numeric@1.1` | 按数值列排序 | `{"column":"value"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\nb,2\nb,2\n"` |
| `csv_filter_equal@1.1` | 保留字段匹配的 CSV 记录 | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\n"` |
| `csv_filter_not_equal@1.1` | 排除字段匹配的 CSV 记录 | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\nb,2\n"` |
| `csv_filter_contains@1.1` | 按子串筛选 CSV 记录 | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\n"` |
| `csv_trim@1.1` | 去除 CSV 单元格首尾空白 | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `csv_uppercase@1.1` | CSV 指定列转大写 | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nB,2\nA,1\nB,2\n"` |
| `csv_lowercase@1.1` | CSV 指定列转小写 | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `csv_fill_empty@1.1` | 填充 CSV 空单元格 | `{"value":"NA"}` | `"name,value\na,\n"` | `"name,value\na,NA\n"` |
| `csv_add_constant@1.1` | 添加常量 CSV 列 | `{"column":"region","value":"east"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value,region\nb,2,east\na,1,east\nb,2,east\n"` |
| `csv_to_ndjson@1.1` | CSV 转 NDJSON 对象 | `{}` | `"a,b\n1,2\n"` | `"{\"a\":\"1\",\"b\":\"2\"}\n"` |
| `csv_count@1.1` | 统计 CSV 数据行数 | `{}` | `"a\nx\ny\n"` | `"{\"rows\":2}"` |
| `records_identity@1.1` | 透传 Arrow 记录批 | `{}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[2,1,2]}` |
| `records_select@1.1` | 选择 Arrow 列 | `{"columns":["name"]}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"]}` |
| `records_drop@1.1` | 删除 Arrow 列 | `{"columns":["name"]}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"value":[2,1,2]}` |
| `records_rename@1.1` | 重命名 Arrow 列 | `{"old":"name","new":"label"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"label":["b","a","b"],"value":[2,1,2]}` |
| `records_reverse_columns@1.1` | 反转列顺序 | `{}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"value":[2,1,2],"name":["b","a","b"]}` |
| `records_sort_columns@1.1` | 按列名排序 | `{}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[2,1,2]}` |
| `records_filter_equal@1.1` | 按字段相等过滤记录 | `{"column":"name","value":"a"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["a"],"value":[1]}` |
| `records_filter_not_equal@1.1` | 按字段不等过滤记录 | `{"column":"name","value":"a"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","b"],"value":[2,2]}` |
| `records_filter_greater@1.1` | 过滤大于阈值的记录 | `{"column":"value","value":1}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","b"],"value":[2,2]}` |
| `records_filter_less@1.1` | 过滤小于阈值的记录 | `{"column":"value","value":2}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["a"],"value":[1]}` |
| `records_filter_nonnull@1.1` | 保留字段非空的记录 | `{"column":"value"}` | `{"value":[1,null]}` | `{"value":[1]}` |
| `records_filter_null@1.1` | 保留字段为空的记录 | `{"column":"value"}` | `{"value":[1,null]}` | `{"value":[null]}` |
| `records_uppercase@1.1` | Arrow 字符串列转大写 | `{"column":"name"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["B","A","B"],"value":[2,1,2]}` |
| `records_lowercase@1.1` | Arrow 字符串列转小写 | `{"column":"name"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[2,1,2]}` |
| `records_trim@1.1` | 去除 Arrow 字符串列首尾空白 | `{"column":"name"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[2,1,2]}` |
| `records_fill_null@1.1` | 填充空值 | `{"column":"value","value":0}` | `{"value":[1,null]}` | `{"value":[1,0]}` |
| `records_add@1.1` | 数值列加常量 | `{"column":"value","value":1}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[3,2,3]}` |
| `records_multiply@1.1` | 数值列乘常量 | `{"column":"value","value":2}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[4,2,4]}` |
| `records_absolute@1.1` | 数值列取绝对值 | `{"column":"value"}` | `{"value":[-2,1]}` | `{"value":[2,1]}` |
| `records_negate@1.1` | 数值列取相反数 | `{"column":"value"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[-2,-1,-2]}` |
| `bytes_sha256@1.1` | 计算数据摘要 | `{}` | `"abc"` | `"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad\n"` |
| `bytes_sha512@1.1` | 计算数据摘要 | `{}` | `"abc"` | `"ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f\n"` |
| `bytes_sha3_256@1.1` | 计算数据摘要 | `{}` | `"abc"` | `"3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532\n"` |
| `bytes_blake2b@1.1` | 计算数据摘要 | `{}` | `"abc"` | `"ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d17d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923\n"` |
| `bytes_base64_encode@1.1` | Base64 编码 | `{}` | `"abc"` | `"YWJj"` |
| `bytes_base64_decode@1.1` | 严格 Base64 解码 | `{}` | `"YWJj"` | `"abc"` |
| `bytes_hex_encode@1.1` | 十六进制编码 | `{}` | `"abc"` | `"616263"` |
| `bytes_hex_decode@1.1` | 十六进制解码 | `{}` | `"616263"` | `"abc"` |
| `bytes_gzip_compress@1.1` | 可复现 GZIP 压缩 | `{}` | `"abc"` | `hex:1f8b08000000000002034b4c4a0600c241243503000000` |
| `bytes_count@1.1` | 统计输入字节数 | `{}` | `"abc"` | `"{\"bytes\":3}"` |

[同进程合并执行](../../../docs/operator-fusion.md)：构建的模块同时暴露线程安全的 `process(ctx)`，同节点、同环境的步骤可共享一个 Python 解释器。
