# Common operators

[中文](README_zh.md)

This collection provides 100 independently named operators: 30 text, 20 JSON, 20 CSV, 20 Arrow record and 10 binary operators. All use the `python` executor, sharing the `dacp/operator-python:1.0` environment and one small wheel. Each operator has its own entrypoint and an immutable version selected by `--version` (default `1.1`) and downloadable sources; 100 container images are unnecessary. The repository does not execute operators; DACP nodes launch them.

Every operator reads `source` and writes `result`. Inputs and outputs use `bytes`, except `records_*`, which use Arrow `records`. Text and CSV are UTF-8; JSON output is UTF-8. Record transforms preserve structured records and run batch by batch, including filtering, column selection and calculations; there is no cross-batch sorting or aggregation. Empty batches are supported and inputs must be consumed through EOS. Parameters shown below are required; parameterless operators take `{}` and unknown parameters fail.

Document transforms require one complete Item, read through Arrow streams and processed in memory, with a default maximum of 16 MiB. Set `max_input_bytes` to a lower limit (1 through 16777216). Oversized input fails rather than being truncated. These operators suit small documents, not unbounded streams. Record operators process individual batches and are not subject to the document size limit. Results still travel as DACP streams without saving server-side result files.

CSV requires a header with unique column names and consistent row widths; output uses LF. CSV-to-JSON conversion preserves cell values as strings. JSON array sorting requires comparable elements. Missing fields, invalid UTF-8 or incompatible types fail. Use unoccupied destination names when renaming or adding columns to avoid duplicate fields. GZIP output has no timestamp for reproducible content verification.

## Build, validate and publish

Run from the project root, with the existing `identity_python@1.1` runtime manifest available in the catalog. Building reuses its pinned image digest and verified archive without exporting the image again. Tests require PyArrow.

```bash
.venv/bin/python scripts/test-common-operators.py
.venv/bin/python examples/operators/common/build.py --runtime-catalog .operator-catalog
.venv/bin/python examples/operators/common/build.py --runtime-catalog .operator-catalog
```

Build output is `examples/operators/common/target/catalog`. HTTP publication reuses shared blobs and submits each manifest separately; each successful publication immediately becomes browsable. Repeated runs verify identical names and versions without overwriting them; bump the version when changing code. The token is used only by the API, never displayed on the website. Use HTTPS for remote publication. The source archive includes implementations, entrypoints, SDK and bilingual instructions. No unverified license declaration is added.

## Workflow example

Use this step in an existing task template, replacing `node` and `uri` with an actual node and Item, then submit through a Union or node client. `params` also supplies the parameter object for shell `run`; see the [operator authoring guide](../../../docs/operator-authoring.md) for complete commands.

```yaml
id: clean_text
title: Uppercase text
node: dacp://localhost:18861
operator: text_uppercase@1.1
inputs:
  source:
    uri: dacp://localhost:18861/datasets/example/items/message
params: {}
```

## Operators, parameters and input/output examples

Binary values use `hex:` notation; records use JSON objects mapping column names to arrays. `\n` denotes a newline; quotes in the table are example notation, not part of the raw byte stream.

| Operator | Purpose | Parameters | Input | Output |
|---|---|---|---|---|
| `text_uppercase@1.1` | Unicode uppercase | `{}` | `"Hello 世界"` | `"HELLO 世界"` |
| `text_lowercase@1.1` | Unicode lowercase | `{}` | `"Hello"` | `"hello"` |
| `text_casefold@1.1` | Unicode case folding | `{}` | `"Straße"` | `"strasse"` |
| `text_titlecase@1.1` | Title case | `{}` | `"hello world"` | `"Hello World"` |
| `text_capitalize@1.1` | Capitalize sentence | `{}` | `"hELLO"` | `"Hello"` |
| `text_swapcase@1.1` | Swap letter case | `{}` | `"AbC"` | `"aBc"` |
| `text_strip@1.1` | Trim surrounding whitespace | `{}` | `" a \n"` | `"a"` |
| `text_lstrip@1.1` | Trim leading whitespace | `{}` | `" a "` | `"a "` |
| `text_rstrip@1.1` | Trim trailing whitespace | `{}` | `" a "` | `" a"` |
| `text_normalize_spaces@1.1` | Collapse whitespace | `{}` | `"a  b\nc"` | `"a b c"` |
| `text_normalize_newlines@1.1` | Normalize CRLF and CR | `{}` | `"a\r\nb\r"` | `"a\nb\n"` |
| `text_remove_blank_lines@1.1` | Remove blank lines | `{}` | `"a\n \nb"` | `"a\nb"` |
| `text_trim_lines@1.1` | Trim each line | `{}` | `" a \n b "` | `"a\nb"` |
| `text_sort_lines@1.1` | Sort lines lexically | `{}` | `"b\na"` | `"a\nb"` |
| `text_reverse_lines@1.1` | Reverse line order | `{}` | `"a\nb"` | `"b\na"` |
| `text_unique_lines@1.1` | Deduplicate lines preserving order | `{}` | `"a\nb\na"` | `"a\nb"` |
| `text_number_lines@1.1` | Number lines starting at one | `{}` | `"a\nb"` | `"1\ta\n2\tb"` |
| `text_head_lines@1.1` | First n lines | `{"n":2}` | `"a\nb\nc"` | `"a\nb"` |
| `text_tail_lines@1.1` | Last n lines | `{"n":2}` | `"a\nb\nc"` | `"b\nc"` |
| `text_filter_lines@1.1` | Keep lines containing literal text | `{"text":"app"}` | `"apple\npear"` | `"apple"` |
| `text_exclude_lines@1.1` | Exclude lines containing literal text | `{"text":"app"}` | `"apple\npear"` | `"pear"` |
| `text_replace@1.1` | Replace literal substring | `{"old":",","new":";"}` | `"a,b"` | `"a;b"` |
| `text_prefix_lines@1.1` | Prefix each line | `{"prefix":"> "}` | `"a\nb"` | `"> a\n> b"` |
| `text_suffix_lines@1.1` | Suffix each line | `{"suffix":"!"}` | `"a\nb"` | `"a!\nb!"` |
| `text_normalize_nfc@1.1` | Unicode NFC normalization | `{}` | `"é"` | `"é"` |
| `text_normalize_nfkc@1.1` | Unicode NFKC normalization | `{}` | `"ＡＢ"` | `"AB"` |
| `text_html_escape@1.1` | Escape HTML text | `{}` | `"<a>"` | `"&lt;a&gt;"` |
| `text_html_unescape@1.1` | Decode HTML entities | `{}` | `"&amp;"` | `"&"` |
| `text_url_encode@1.1` | Percent encode UTF-8 | `{}` | `"a b/"` | `"a%20b%2F"` |
| `text_url_decode@1.1` | Percent decode UTF-8 | `{}` | `"a%20b"` | `"a b"` |
| `json_identity@1.1` | Validate and compact JSON | `{}` | `"{\"a\":1}"` | `"{\"a\":1}"` |
| `json_pretty@1.1` | Pretty-print JSON | `{}` | `"{\"a\":1}"` | `"{\n  \"a\": 1\n}"` |
| `json_sort_keys@1.1` | Sort object keys recursively | `{}` | `"{\"b\":2,\"a\":1}"` | `"{\"a\":1,\"b\":2}"` |
| `json_keys@1.1` | List object keys | `{}` | `"{\"a\":1}"` | `"[\"a\"]"` |
| `json_values@1.1` | List object values | `{}` | `"{\"a\":1}"` | `"[1]"` |
| `json_entries@1.1` | Convert object to key-value pairs | `{}` | `"{\"a\":1}"` | `"[[\"a\",1]]"` |
| `json_get@1.1` | Get top-level key | `{"key":"a"}` | `"{\"a\":1}"` | `"1"` |
| `json_select@1.1` | Select object keys | `{"keys":["a"]}` | `"{\"a\":1,\"b\":2}"` | `"{\"a\":1}"` |
| `json_drop@1.1` | Drop object keys | `{"keys":["a"]}` | `"{\"a\":1,\"b\":2}"` | `"{\"b\":2}"` |
| `json_rename@1.1` | Rename object key | `{"old":"a","new":"b"}` | `"{\"a\":1}"` | `"{\"b\":1}"` |
| `json_set@1.1` | Set object key | `{"key":"b","value":2}` | `"{\"a\":1}"` | `"{\"a\":1,\"b\":2}"` |
| `json_array_length@1.1` | Count array elements | `{}` | `"[1,2]"` | `"2"` |
| `json_array_reverse@1.1` | Reverse array | `{}` | `"[1,2]"` | `"[2,1]"` |
| `json_array_sort@1.1` | Sort comparable array elements | `{}` | `"[2,1]"` | `"[1,2]"` |
| `json_array_unique@1.1` | Deduplicate JSON array | `{}` | `"[1,1,2]"` | `"[1,2]"` |
| `json_array_head@1.1` | First n array elements | `{"n":2}` | `"[1,2,3]"` | `"[1,2]"` |
| `json_array_flatten@1.1` | Flatten one array level | `{}` | `"[[1],[2,3]]"` | `"[1,2,3]"` |
| `json_pluck@1.1` | Extract key from object array | `{"key":"a"}` | `"[{\"a\":1},{\"a\":2}]"` | `"[1,2]"` |
| `json_filter_equal@1.1` | Filter object array by equality | `{"key":"a","value":2}` | `"[{\"a\":1},{\"a\":2}]"` | `"[{\"a\":2}]"` |
| `json_to_ndjson@1.1` | JSON array to NDJSON | `{}` | `"[{\"a\":1},{\"a\":2}]"` | `"{\"a\":1}\n{\"a\":2}\n"` |
| `csv_identity@1.1` | Validate and normalize CSV | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `csv_select@1.1` | Select CSV columns | `{"columns":["name"]}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name\nb\na\nb\n"` |
| `csv_drop@1.1` | Drop CSV columns | `{"columns":["name"]}` | `"name,value\nb,2\na,1\nb,2\n"` | `"value\n2\n1\n2\n"` |
| `csv_rename@1.1` | Rename CSV column | `{"old":"name","new":"label"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"label,value\nb,2\na,1\nb,2\n"` |
| `csv_head@1.1` | First n CSV rows | `{"n":1}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\n"` |
| `csv_tail@1.1` | Last n CSV rows | `{"n":1}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\n"` |
| `csv_reverse@1.1` | Reverse CSV row order | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `csv_unique@1.1` | Deduplicate CSV rows | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\n"` |
| `csv_sort@1.1` | Sort by text column | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\nb,2\nb,2\n"` |
| `csv_sort_numeric@1.1` | Sort by numeric column | `{"column":"value"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\nb,2\nb,2\n"` |
| `csv_filter_equal@1.1` | Keep matching CSV rows | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\n"` |
| `csv_filter_not_equal@1.1` | Exclude matching CSV rows | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\nb,2\n"` |
| `csv_filter_contains@1.1` | Filter CSV substring | `{"column":"name","value":"a"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\na,1\n"` |
| `csv_trim@1.1` | Trim all CSV cells | `{}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `csv_uppercase@1.1` | Uppercase CSV column | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nB,2\nA,1\nB,2\n"` |
| `csv_lowercase@1.1` | Lowercase CSV column | `{"column":"name"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value\nb,2\na,1\nb,2\n"` |
| `csv_fill_empty@1.1` | Fill empty CSV cells | `{"value":"NA"}` | `"name,value\na,\n"` | `"name,value\na,NA\n"` |
| `csv_add_constant@1.1` | Add constant CSV column | `{"column":"region","value":"east"}` | `"name,value\nb,2\na,1\nb,2\n"` | `"name,value,region\nb,2,east\na,1,east\nb,2,east\n"` |
| `csv_to_ndjson@1.1` | CSV to JSON objects | `{}` | `"a,b\n1,2\n"` | `"{\"a\":\"1\",\"b\":\"2\"}\n"` |
| `csv_count@1.1` | Count CSV data rows | `{}` | `"a\nx\ny\n"` | `"{\"rows\":2}"` |
| `records_identity@1.1` | Pass Arrow record batches | `{}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[2,1,2]}` |
| `records_select@1.1` | Select Arrow columns | `{"columns":["name"]}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"]}` |
| `records_drop@1.1` | Drop Arrow columns | `{"columns":["name"]}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"value":[2,1,2]}` |
| `records_rename@1.1` | Rename Arrow column | `{"old":"name","new":"label"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"label":["b","a","b"],"value":[2,1,2]}` |
| `records_reverse_columns@1.1` | Reverse column order | `{}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"value":[2,1,2],"name":["b","a","b"]}` |
| `records_sort_columns@1.1` | Sort columns by name | `{}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[2,1,2]}` |
| `records_filter_equal@1.1` | Filter records by equality | `{"column":"name","value":"a"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["a"],"value":[1]}` |
| `records_filter_not_equal@1.1` | Filter records by inequality | `{"column":"name","value":"a"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","b"],"value":[2,2]}` |
| `records_filter_greater@1.1` | Filter records above threshold | `{"column":"value","value":1}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","b"],"value":[2,2]}` |
| `records_filter_less@1.1` | Filter records below threshold | `{"column":"value","value":2}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["a"],"value":[1]}` |
| `records_filter_nonnull@1.1` | Keep nonnull records | `{"column":"value"}` | `{"value":[1,null]}` | `{"value":[1]}` |
| `records_filter_null@1.1` | Keep null records | `{"column":"value"}` | `{"value":[1,null]}` | `{"value":[null]}` |
| `records_uppercase@1.1` | Uppercase Arrow string column | `{"column":"name"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["B","A","B"],"value":[2,1,2]}` |
| `records_lowercase@1.1` | Lowercase Arrow string column | `{"column":"name"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[2,1,2]}` |
| `records_trim@1.1` | Trim Arrow string column | `{"column":"name"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[2,1,2]}` |
| `records_fill_null@1.1` | Fill null values | `{"column":"value","value":0}` | `{"value":[1,null]}` | `{"value":[1,0]}` |
| `records_add@1.1` | Add scalar to numeric column | `{"column":"value","value":1}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[3,2,3]}` |
| `records_multiply@1.1` | Multiply numeric column by scalar | `{"column":"value","value":2}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[4,2,4]}` |
| `records_absolute@1.1` | Absolute value of numeric column | `{"column":"value"}` | `{"value":[-2,1]}` | `{"value":[2,1]}` |
| `records_negate@1.1` | Negate numeric column | `{"column":"value"}` | `{"name":["b","a","b"],"value":[2,1,2]}` | `{"name":["b","a","b"],"value":[-2,-1,-2]}` |
| `bytes_sha256@1.1` | Compute sha256 digest | `{}` | `"abc"` | `"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad\n"` |
| `bytes_sha512@1.1` | Compute sha512 digest | `{}` | `"abc"` | `"ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f\n"` |
| `bytes_sha3_256@1.1` | Compute sha3_256 digest | `{}` | `"abc"` | `"3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532\n"` |
| `bytes_blake2b@1.1` | Compute blake2b digest | `{}` | `"abc"` | `"ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d17d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923\n"` |
| `bytes_base64_encode@1.1` | Base64 encode | `{}` | `"abc"` | `"YWJj"` |
| `bytes_base64_decode@1.1` | Strict Base64 decode | `{}` | `"YWJj"` | `"abc"` |
| `bytes_hex_encode@1.1` | Hexadecimal encode | `{}` | `"abc"` | `"616263"` |
| `bytes_hex_decode@1.1` | Hexadecimal decode | `{}` | `"616263"` | `"abc"` |
| `bytes_gzip_compress@1.1` | Deterministic GZIP compression | `{}` | `"abc"` | `hex:1f8b08000000000002034b4c4a0600c241243503000000` |
| `bytes_count@1.1` | Count input bytes | `{}` | `"abc"` | `"{\"bytes\":3}"` |

[In-process fusion](../../../docs/operator-fusion.md): generated modules expose a thread-safe `process(ctx)`, allowing steps on the same node and environment to share one Python interpreter.

The repository accepts source only. Local builds in these examples are for development verification; use the [source submission and review workflow](../../../docs/operator-submission.md) for public publication.
