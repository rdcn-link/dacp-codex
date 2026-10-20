# Common Scala operators

[中文](README.md)

This collection contains 100 Scala operators: 50 text, 20 JSON, 20 CSV and 10 binary operators, with the `scala_` prefix and version `1.0`. All use the `jvm` executor, one shared thin JAR (including the small SDK, with Arrow, Jackson and Scala supplied by the runtime) and the `dacp/operator-jvm:1.1` runtime. Each has an independent JVM entrypoint and repository manifest, with downloadable sources. Execution logic is Scala; Python build scripts only generate metadata and publication materials.

Every operator reads `source` and writes `result`, both using `bytes`. Text, CSV and JSON use UTF-8. The table supplies required parameter examples; use `{}` for parameterless operators. Unknown or missing parameters fail. Arrow input streams are fully consumed through EOS, then a complete Item is transformed in memory, with a default 16 MiB limit. `max_input_bytes` accepts an integer from 1 through 16777216. Oversized input fails without truncation. Output travels through DACP result streams without saving server-side result files. These bounded document operators are not suitable for unbounded streams; see the [Python record operators](../common/README_en.md) for batch processing.

CSV requires a unique header and consistent row widths. Quotes, escaped quotes, embedded newlines and CRLF are supported; output uses LF. CSV-to-JSON preserves strings. Use unoccupied names when renaming or adding columns. JSON array sorting supports numbers or strings; missing fields, incompatible types and invalid UTF-8 fail. `text_fold_german` only lowercases and expands ß; `text_html_unescape_basic` only decodes the five entities `&lt;`, `&gt;`, `&quot;`, `&#x27;` and `&amp;`. Character reversal and counting use Unicode code points, not grapheme clusters. Textual digest and count results end in LF.

## Build and publish individually

Requires JDK 17, sbt, Python 3 and PyArrow. Publish local dependencies from the project root first, sequentially rather than running concurrent builds sharing targets. The pinned runtime digest and archive are read and verified from the existing `identity@1.3` catalog manifest; no image export is needed for each operator.

```bash
(cd dacp-plugin-api && sbt publishLocal)
(cd dacp-operator-sdk && sbt publishLocal)
.venv/bin/python examples/operators/common-scala/build.py
export DACP_REPOSITORY_PUBLISH_TOKEN="$(cat .operator-catalog/.publish-token)"
.venv/bin/python examples/operators/common-scala/build.py --publish http://127.0.0.1:8820
```

The builder generates independent entrypoints, compiles the thin JAR and verifies 100 input/output examples. Output is `examples/operators/common-scala/target/catalog`. Publication shares code, runtime and source blobs, submitting each operator through the token-authenticated HTTP API; every successful entry is immediately browsable. Versions cannot be overwritten; use a new version when changing code. Repeated publication accepts only identical manifests. Use HTTPS remotely. Downloadable sources contain implementation, build files, entrypoints, test samples and bilingual instructions. They depend on this project's SDK and build helpers; build from the project root using the commands above.

Reference `operator: scala_text_uppercase@1.0` in an existing workflow, bind input to `source` and consume output with `port: result`. For example, `scala_text_replace@1.0` takes `params: {old: ",", new: ";"}`. See the [complete Union workflow](../../union/job.yaml) and [operator authoring guide](../../../docs/operator-authoring_en.md).

## Parameters and input/output examples

Quotes denote example strings rather than literal input, `\n` denotes a newline, and binary values use `hex:`.

| Operator | Purpose | Parameters | Input | Output |
|---|---|---|---|---|
| `scala_text_uppercase@1.0` | Unicode uppercase | `{}` | `"Hello 世界"` | `"HELLO 世界"` |
| `scala_text_lowercase@1.0` | Unicode lowercase | `{}` | `"Hello"` | `"hello"` |
| `scala_text_titlecase@1.0` | Title case | `{}` | `"hello world"` | `"Hello World"` |
| `scala_text_capitalize@1.0` | Capitalize sentence | `{}` | `"hELLO"` | `"Hello"` |
| `scala_text_swapcase@1.0` | Swap letter case | `{}` | `"AbC"` | `"aBc"` |
| `scala_text_strip@1.0` | Trim surrounding whitespace | `{}` | `" a \n"` | `"a"` |
| `scala_text_lstrip@1.0` | Trim leading whitespace | `{}` | `" a "` | `"a "` |
| `scala_text_rstrip@1.0` | Trim trailing whitespace | `{}` | `" a "` | `" a"` |
| `scala_text_normalize_spaces@1.0` | Collapse whitespace | `{}` | `"a  b\nc"` | `"a b c"` |
| `scala_text_normalize_newlines@1.0` | Normalize CRLF and CR | `{}` | `"a\r\nb\r"` | `"a\nb\n"` |
| `scala_text_remove_blank_lines@1.0` | Remove blank lines | `{}` | `"a\n \nb"` | `"a\nb"` |
| `scala_text_trim_lines@1.0` | Trim each line | `{}` | `" a \n b "` | `"a\nb"` |
| `scala_text_sort_lines@1.0` | Sort lines lexically | `{}` | `"b\na"` | `"a\nb"` |
| `scala_text_reverse_lines@1.0` | Reverse line order | `{}` | `"a\nb"` | `"b\na"` |
| `scala_text_unique_lines@1.0` | Deduplicate lines preserving order | `{}` | `"a\nb\na"` | `"a\nb"` |
| `scala_text_number_lines@1.0` | Number lines starting at one | `{}` | `"a\nb"` | `"1\ta\n2\tb"` |
| `scala_text_head_lines@1.0` | First n lines | `{"n":2}` | `"a\nb\nc"` | `"a\nb"` |
| `scala_text_tail_lines@1.0` | Last n lines | `{"n":2}` | `"a\nb\nc"` | `"b\nc"` |
| `scala_text_filter_lines@1.0` | Keep lines containing literal text | `{"text":"app"}` | `"apple\npear"` | `"apple"` |
| `scala_text_exclude_lines@1.0` | Exclude lines containing literal text | `{"text":"app"}` | `"apple\npear"` | `"pear"` |
| `scala_text_replace@1.0` | Replace literal substring | `{"old":",","new":";"}` | `"a,b"` | `"a;b"` |
| `scala_text_prefix_lines@1.0` | Prefix each line | `{"prefix":"> "}` | `"a\nb"` | `"> a\n> b"` |
| `scala_text_suffix_lines@1.0` | Suffix each line | `{"suffix":"!"}` | `"a\nb"` | `"a!\nb!"` |
| `scala_text_normalize_nfc@1.0` | Unicode NFC normalization | `{}` | `"é"` | `"é"` |
| `scala_text_normalize_nfkc@1.0` | Unicode NFKC normalization | `{}` | `"ＡＢ"` | `"AB"` |
| `scala_text_html_escape@1.0` | Escape HTML text | `{}` | `"<a>"` | `"&lt;a&gt;"` |
| `scala_text_url_encode@1.0` | Percent encode UTF-8 | `{}` | `"a b/"` | `"a%20b%2F"` |
| `scala_text_url_decode@1.0` | Percent decode UTF-8 | `{}` | `"a%20b"` | `"a b"` |
| `scala_json_identity@1.0` | Validate and compact JSON | `{}` | `"{\"a\":1}"` | `"{\"a\":1}"` |
| `scala_json_pretty@1.0` | Pretty-print JSON | `{}` | `"{\"a\":1}"` | `"{\n  \"a\" : 1\n}"` |
| `scala_json_sort_keys@1.0` | Sort object keys recursively | `{}` | `"{\"b\":2,\"a\":1}"` | `"{\"a\":1,\"b\":2}"` |
| `scala_json_keys@1.0` | List object keys | `{}` | `"{\"a\":1}"` | `"[\"a\"]"` |
| `scala_json_values@1.0` | List object values | `{}` | `"{\"a\":1}"` | `"[1]"` |
| `scala_json_entries@1.0` | Convert object to key-value pairs | `{}` | `"{\"a\":1}"` | `"[[\"a\",1]]"` |
| `scala_json_get@1.0` | Get top-level key | `{"key":"a"}` | `"{\"a\":1}"` | `"1"` |
| `scala_json_select@1.0` | Select object keys | `{"keys":["a"]}` | `"{\"a\":1,\"b\":2}"` | `"{\"a\":1}"` |
| `scala_json_drop@1.0` | Drop object keys | `{"keys":["a"]}` | `"{\"a\":1,\"b\":2}"` | `"{\"b\":2}"` |
| `scala_json_rename@1.0` | Rename object key | `{"old":"a","new":"b"}` | `"{\"a\":1}"` | `"{\"b\":1}"` |
| `scala_json_set@1.0` | Set object key | `{"key":"b","value":2}` | `"{\"a\":1}"` | `"{\"a\":1,\"b\":2}"` |
| `scala_json_array_length@1.0` | Count array elements | `{}` | `"[1,2]"` | `"2"` |
| `scala_json_array_reverse@1.0` | Reverse array | `{}` | `"[1,2]"` | `"[2,1]"` |
| `scala_json_array_sort@1.0` | Sort comparable array elements | `{}` | `"[2,1]"` | `"[1,2]"` |
| `scala_json_array_unique@1.0` | Deduplicate JSON array | `{}` | `"[1,1,2]"` | `"[1,2]"` |
| `scala_json_array_head@1.0` | First n array elements | `{"n":2}` | `"[1,2,3]"` | `"[1,2]"` |
| `scala_json_array_flatten@1.0` | Flatten one array level | `{}` | `"[[1],[2,3]]"` | `"[1,2,3]"` |
| `scala_json_pluck@1.0` | Extract key from object array | `{"key":"a"}` | `"[{\"a\":1},{\"a\":2}]"` | `"[1,2]"` |
| `scala_json_filter_equal@1.0` | Filter object array by equality | `{"key":"a","value":2}` | `"[{\"a\":1},{\"a\":2}]"` | `"[{\"a\":2}]"` |
| `scala_json_to_ndjson@1.0` | JSON array to NDJSON | `{}` | `"[{\"a\":1},{\"a\":2}]"` | `"{\"a\":1}\n{\"a\":2}\n"` |
| `scala_csv_identity@1.0` | Validate and normalize CSV | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `scala_csv_select@1.0` | Select CSV columns | `{"columns":["name"]}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name\nb\na\nb\n"` |
| `scala_csv_drop@1.0` | Drop CSV columns | `{"columns":["name"]}` | `"name,value\nb,2\na,1\nb,2\n"` | `"value\n2\n1\n2\n"` |
| `scala_csv_rename@1.0` | Rename CSV column | `{"old":"name","new":"label"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"label,value\nb,2\na,1\nb,2\n"` |
| `scala_csv_head@1.0` | First n CSV rows | `{"n":1}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\n"` |
| `scala_csv_tail@1.0` | Last n CSV rows | `{"n":1}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\n"` |
| `scala_csv_reverse@1.0` | Reverse CSV row order | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `scala_csv_unique@1.0` | Deduplicate CSV rows | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\n"` |
| `scala_csv_sort@1.0` | Sort by text column | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\nb,2\nb,2\n"` |
| `scala_csv_sort_numeric@1.0` | Sort by numeric column | `{"column":"value"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\nb,2\nb,2\n"` |
| `scala_csv_filter_equal@1.0` | Keep matching CSV rows | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\n"` |
| `scala_csv_filter_not_equal@1.0` | Exclude matching CSV rows | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\nb,2\n"` |
| `scala_csv_filter_contains@1.0` | Filter CSV substring | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\n"` |
| `scala_csv_trim@1.0` | Trim all CSV cells | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `scala_csv_uppercase@1.0` | Uppercase CSV column | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nB,2\nA,1\nB,2\n"` |
| `scala_csv_lowercase@1.0` | Lowercase CSV column | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `scala_csv_fill_empty@1.0` | Fill empty CSV cells | `{"value":"NA"}` | `"name,value\na,\n"` | `"name,value\na,NA\n"` |
| `scala_csv_add_constant@1.0` | Add constant CSV column | `{"column":"region","value":"east"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value,region\nb,2,east\na,1,east\nb,2,east\n"` |
| `scala_csv_to_ndjson@1.0` | CSV to JSON objects | `{}` | `"a,b\n1,2\n"` | `"{\"a\":\"1\",\"b\":\"2\"}\n"` |
| `scala_csv_count@1.0` | Count CSV data rows | `{}` | `"a\nx\ny\n"` | `"{\"rows\":2}"` |
| `scala_bytes_sha256@1.0` | Compute sha256 digest | `{}` | `"abc"` | `"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad\n"` |
| `scala_bytes_sha512@1.0` | Compute sha512 digest | `{}` | `"abc"` | `"ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f\n"` |
| `scala_bytes_sha3_256@1.0` | Compute sha3_256 digest | `{}` | `"abc"` | `"3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532\n"` |
| `scala_bytes_base64_encode@1.0` | Base64 encode | `{}` | `"abc"` | `"YWJj"` |
| `scala_bytes_base64_decode@1.0` | Strict Base64 decode | `{}` | `"YWJj"` | `"abc"` |
| `scala_bytes_hex_encode@1.0` | Hexadecimal encode | `{}` | `"abc"` | `"616263"` |
| `scala_bytes_hex_decode@1.0` | Hexadecimal decode | `{}` | `"616263"` | `"abc"` |
| `scala_bytes_count@1.0` | Count input bytes | `{}` | `"abc"` | `"{\"bytes\":3}"` |
| `scala_text_fold_german@1.0` | Lowercase and fold German sharp s | `{}` | `"Straße"` | `"strasse"` |
| `scala_text_html_unescape_basic@1.0` | Decode five basic HTML entities | `{}` | `"&amp;"` | `"&"` |
| `scala_bytes_sha384@1.0` | SHA-384 digest | `{}` | `"abc"` | `"cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7\n"` |
| `scala_bytes_crc32@1.0` | CRC32 decimal checksum | `{}` | `"abc"` | `"891568578\n"` |
| `scala_text_reverse_characters@1.0` | Reverse Unicode code points | `{}` | `"ab世界"` | `"界世ba"` |
| `scala_text_count_characters@1.0` | Count Unicode code points | `{}` | `"ab世界"` | `"4\n"` |
| `scala_text_count_lines@1.0` | Count lines | `{}` | `"a\nb\n"` | `"2\n"` |
| `scala_text_count_words@1.0` | Count whitespace-separated words | `{}` | `"a b\nc"` | `"3\n"` |
| `scala_text_remove_digits@1.0` | Remove ASCII digits | `{}` | `"a1b2"` | `"ab"` |
| `scala_text_keep_digits@1.0` | Keep ASCII digits | `{}` | `"a1b2"` | `"12"` |
| `scala_text_remove_ascii_punctuation@1.0` | Remove ASCII punctuation | `{}` | `"a,b!"` | `"ab"` |
| `scala_text_tabs_to_spaces@1.0` | Expand tabs to four spaces | `{}` | `"a\tb"` | `"a    b"` |
| `scala_text_spaces_to_tabs@1.0` | Four spaces to tab | `{}` | `"a    b"` | `"a\tb"` |
| `scala_text_join_lines@1.0` | Join lines with separator | `{"separator":","}` | `"a\nb"` | `"a,b"` |
| `scala_text_split_words@1.0` | Split words into lines | `{}` | `"a b\tc"` | `"a\nb\nc"` |
| `scala_text_sort_lines_desc@1.0` | Sort lines descending | `{}` | `"a\nb"` | `"b\na"` |
| `scala_text_nonempty_count@1.0` | Count nonblank lines | `{}` | `"a\n \nb"` | `"2\n"` |
| `scala_text_remove_bom@1.0` | Remove leading Unicode BOM | `{}` | `"﻿abc"` | `"abc"` |
| `scala_text_ensure_newline@1.0` | Ensure final LF | `{}` | `"abc"` | `"abc\n"` |
| `scala_text_remove_newlines@1.0` | Remove CR and LF | `{}` | `"a\r\nb"` | `"ab"` |
| `scala_text_ascii_only@1.0` | Keep ASCII characters | `{}` | `"abc世界"` | `"abc"` |
| `scala_text_normalize_nfd@1.0` | Unicode NFD normalization | `{}` | `"é"` | `"é"` |
| `scala_text_normalize_nfkd@1.0` | Unicode NFKD normalization | `{}` | `"Ａé"` | `"Aé"` |
| `scala_text_strip_accents@1.0` | Remove Unicode combining marks | `{}` | `"café"` | `"cafe"` |
