# Records queries: filter, projection, and limit

Clients can attach a records scan to an Item stream. The server executes **filter → projection → limit**. Only matching records and requested columns cross the network. Logical Item metadata and `count(path)` still describe the complete Item; they do not reflect a scan. Bytes views reject scan options.

## Python

```python
from dacp import Source, col

with Source.open("dacp://localhost:8815/data1") as ds:
    with ds.open(
        "/reports/orders",
        predicate=(col("id") > 1) & col("amount").is_not_null(),
        columns=["amount"],
        limit=100,
    ) as stream:
        records = stream.as_records()
        try:
            for batch in records:
                print(batch.to_pydict())
        finally:
            records.close()
```

Use parentheses around comparisons combined with `&` and `|`; negate a predicate with `~`. Python `and`, `or`, and `not` are not expression builders. Use `col("id").isin([1, 2])`, `.is_null()`, or `.is_not_null()` for membership and null checks.

## Scala

```scala
import dacp.client.Source
import dacp.client.Expressions.col
import scala.util.Using

Using.resource(Source.open("dacp://localhost:8815/data1")) { ds =>
  Using.resource(ds.open(
    "/reports/orders",
    predicate = Some((col("id") > 1) && col("amount").isNotNull),
    columns = Some(Seq("amount")),
    limit = Some(100L)
  )) { item =>
    Using.resource(item.asRecords()) { records =>
      while (records.next()) println(records.root.contentToTSVString())
    }
  }
}
```

Scala equality and inequality builders are `===` and `=!=`; boolean builders are `&&`, `||`, and unary `!`. Membership is `.isin(Seq(1, 2))`; null checks are `.isNull` and `.isNotNull`. Both SDKs use the same protobuf expression tree; raw SQL is never accepted.

## SQL in dacp-shell

Start the shell against a running DACP server (with `DACP_CA`, `DACP_CERT`, and `DACP_KEY` configured):

```bash
dacp-shell dacp://localhost:8815
# shell:
# datasets
# use data1
```

Enter a complete single-line query at `dacp-shell>`:

```sql
SELECT amount FROM "/reports/orders" WHERE id > 1 LIMIT 10
SELECT amount FROM 2 WHERE id > 1 LIMIT 10
SELECT id, amount FROM "/reports/orders" WHERE NOT (id IN (3, 4)) AND amount IS NOT NULL
SELECT * FROM "/events" WHERE name = 'beta'
```

SQLGlot parses the statement locally into the existing typed scan expression; the server receives no SQL text. The source is exactly one double-quoted absolute public Item path in the current Dataset, or an unquoted positive Item number from the latest `ls` result. For example, `FROM 2` selects row 2 of that listing. `ls PATH` replaces the numbered listing; numbers are resolved locally to logical paths and never sent as source identifiers to the server. Zero and out-of-range numbers fail before opening a stream. Single quotes delimit string literals, with doubled quotes for escaping (`'O''Brien'`). Double quotes also allow column names containing spaces or SQL keywords. Column names retain DACP's exact, case-sensitive matching; SQL keywords are case insensitive. A trailing semicolon is optional.

Supported syntax:

- `SELECT *` or a unique list of column names.
- Optional `WHERE` using `=`, `<>`/`!=`, `<`, `<=`, `>`, `>=`, parentheses, `AND`, `OR`, `NOT`, `IN`/`NOT IN`, and `IS NULL`/`IS NOT NULL`.
- String, boolean, and signed numeric literals. Numbers are typed against the Item's public records schema: floating-point columns receive floating literals, while decimal columns retain exact decimal text. No string-to-number conversions occur.
- Typed date/time literals, such as `DATE '2026-01-02'`, `TIMESTAMP '2026-01-02 12:34:56'`, and `TIMESTAMP WITH TIME ZONE '2026-01-02 12:34:56+02:00'`. The corresponding literal-only `CAST` forms are also accepted.
- Optional nonnegative integer `LIMIT`, including zero.

Only records views support SQL. Numeric fractional literals require the Item's records schema in metadata. Native structured plugins should advertise records first when supplying that schema. Queries without fractional literals can still be constructed when the metadata does not provide a records schema; the server validates them against the streaming schema.

Results use the shell's bordered records table. Empty results keep their selected column names. `--rows` remains a display cap: the shell sends `min(SQL LIMIT, --rows)` to the server, or `--rows` when SQL `LIMIT` is omitted. For larger previews, start the shell with a larger `--rows` value. `help select` describes the command.

The initial implementation rejects joins, aliases, qualified column references, CTEs, subqueries, multiple statements, sorting, grouping, aggregation, computed columns, arbitrary functions, and offset. Unsupported clauses are never silently discarded. Missing/invalid paths, unknown columns, invalid literal types, and sources without records produce errors while leaving the shell connected.

## Semantics and extension implementations

See [XStream records scans](https://github.com/rdcn-link/xstream-codex/blob/main/docs/records-scans.md) for expression semantics, source pushdown and plugin contracts. DACP handles request decoding, authorization and transport; the data layer executes the scan.
