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

## Expression semantics

- References address top-level public Arrow field names, exactly and case sensitively. Nested field traversal, column-to-column comparisons, arithmetic, sorting, aggregation, and offset are outside this implementation.
- Supported comparisons are `eq`, `ne`, `lt`, `le`, `gt`, and `ge`, plus `in`, `is_null`, and `is_not_null`. Boolean expressions support `and`, `or`, and `not`.
- Comparisons with null records evaluate to unknown. Boolean logic has three values: true, false, unknown. Only true matches. For example, `NOT (temperature > 20)` also excludes null temperatures. Use explicit null checks to include them.
- Null literals and null members in `in` are invalid. Empty `in` is false, including for null records.
- Literals carry an explicit type and canonical text, preserving decimal/integer precision. Integer columns require integer literals within the column's range. Decimal columns accept integers and decimals representable at the declared precision and scale. Floating-point columns accept integers and finite floating literals. No string-to-number conversions occur.
- Strings compare case sensitively in Unicode scalar order, without locale collation or normalization. Booleans order false before true. Floating-point NaN is unequal to every ordinary literal and fails ordered comparisons; signed zeroes compare equal. NaN/infinite literals are rejected.
- Dates use calendar dates. Timezone-aware timestamps compare as UTC instants; timestamps without a timezone compare as local date-times. Mixing those categories is invalid. Python uses `date`, `datetime`, and `Decimal`; Scala uses `LocalDate`, `LocalDateTime`, `Instant`/`OffsetDateTime`, and `BigDecimal`.
- Binary, list, map, struct, and other unsupported types can be projected and tested for null, but cannot be compared with ordinary literals. Dictionary-encoded scan streams are currently rejected.

Unknown fields, duplicate/empty projections, invalid literals, negative limits, and malformed expressions fail explicitly. Validation occurs when reading the stream, before record batches are emitted; schema inference may read a sample. Predicates are bounded to 32 levels and 1,024 expression nodes, with at most 10,000 members per `in` expression.

Projection preserves the requested column order. Omit it to return all columns. A predicate may reference a column absent from the output. Limits are nonnegative signed 64-bit integers and apply globally across batches. Zero returns the projected schema without records. The server stops upstream consumption when the limit is reached, releasing source resources. Readers may have already buffered a batch, JDBC fetch window, or Parquet row group. Queries preserve source order; a stable ordering across independent reads is not guaranteed.

## Execution and source pushdown

| Source | Current behavior |
|---|---|
| JDBC SQL | Prepared predicates for integers, decimals, booleans, and null checks; source projection includes output and filter columns. JDBC maximum rows is set only when the complete predicate is handled. |
| DACP Parquet | Avro/Parquet column projection, plus supported signed-integer predicates passed to the Parquet reader for record and available row-group pruning. |
| CSV, NDJSON, Excel | Bounded batch filtering and projection on the server; early termination at the limit. |
| Contributed provider or native structured child | Common streaming fallback, or an equivalent plugin implementation of the scan contract. |

Pushdown is an optimization. The common evaluator checks the original predicate even after SQL or Parquet pushdown. Unsupported source predicates remain in that evaluator. Conjunctions may be partially pushed; disjunctions must remain a safe superset of all matching rows. A limit must never be pushed ahead of a residual filter. The SQL implementation leaves collation-sensitive string comparisons and floating-point comparisons to the common evaluator.

Authorization still applies to the public Item path. Scan requests contain public field names and literal values, never physical file paths, SQL identifiers, or executable query text. Treat predicates as potentially sensitive user data when adding plugin logging.

## Plugin implementation contract

The standalone `dacp-plugin-api` module provides `Literal`, `Predicate`, `Scan`, `BoundScan`, and `ScanStreams`.

- `access.requireView(path, DataViews.Records).open(request, context)` accepts a typed `Scan`; `BatchDatasetAccess.readScan` supplies bounded Arrow batch filtering through `ScanStreams`.
- `FormatHandler.scan(path, options, request, allocator, batch, sample, receiver, demand)` defaults to the same streaming transformation around its records reader. It is used for directory-backed and mixed file Items, including staged archive children.
- Structured-file children use `DataViews.Records`, so a contributed scientific handler can query native records without exposing physical files or flattening arrays.
- Override `scan` to optimize a backend. Validate the full request against the full public schema with `request.bind(schema)` before translating anything. Retain columns required by the predicate until filtering completes. Translation must use quoted identifiers and bound values where applicable.
- An override is responsible for the complete semantics of its request. For partial pushdown, pass source batches through `ScanStreams.stream(request, allocator, receiver) { filtered => ... }`. Start `filtered` with the source root, emit through it, and let it enforce residual filtering, output projection, and the global limit. Keep the original predicate for a safe final check.
- Preserve synchronous exception propagation, demand/cancellation handling, and `finally` cleanup. Reaching a limit unwinds the reader after its current batch. Do not catch and suppress exceptions from `start`, `putNext`, or the demand callback.

See [ScanSpec](../dacp-server/src/test/scala/dacp/ScanSpec.scala) and [SqlSpec](../dacp-server/src/test/scala/dacp/SqlSpec.scala) for transport, semantics, pushdown, and resource-lifecycle tests.
