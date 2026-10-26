# Item-based operator inputs and stream-only outputs

Status: implemented through Flight DoExchange and the Python shell/SDK and Scala SDK. Current ports support bytes and records. Other scientific representations require explicit contract extensions. Each output port has at most one downstream consumer; use distinct ports for duplication. Consume multiple input ports in lexical order.
Source operators may declare `inputs: {}`; tasks must also explicitly provide `inputs: {}`. No trigger Item is needed. Sink operators return an empty output mapping from their mandatory inference method. In the shell, use `run read_item@1.0 --params '{"uri":"dacp://localhost:18861/weather/observations","view":"bytes"}'` without `--input`. Local tasks default to the connected node; `node` may explicitly select placement.


See [Jobs](jobs_en.md) for the format, stream dependencies and sinks.

## User-facing contract

Operators return streams; selected boundaries may be materialized through a separate [execution policy](materialization_en.md). There is no save-to-file option, dataset-result mode, or result-mode selector. The shell and SDK consume streams directly; operators do not publish output datasets or return retained dataset URIs. YAML jobs remain useful for pipelines, with output names identifying connections between streams rather than stored datasets.

Shell usage:

```text
run piflow_csv_parser@1.0 --input source=/observations --view bytes
run piflow_filter@1.0 --input source=/observations --view records --params '{"condition":"CAST(temperature AS REAL) >= 30"}'
submit pipeline.yaml
```

These commands are implemented in dacp-shell. No `--result`, file destination, or saved output dataset is required.

## Operator input and output units

Each operator port accepts or produces one data item, delivered as a stream. An operator may declare multiple named input and output ports. A dataset is a namespace/collection used to locate items; it is not an implicit operator input port or persisted result destination.

```text
convert:
  inputs:  source -> one CSV item
  outputs: result -> one Parquet item stream

join:
  inputs:  left -> one table item, right -> one table item
  outputs: result -> one table item stream
```

Every port declares its accepted or produced representation and applicable schema/format constraints. A data item may be a table, file, image, structured scientific object, or multidimensional array. Restricting ports to items does not require flattening structures, materializing the item, or fitting it in memory. New representations must have explicit shared contracts before implementation.

Input bindings use only `task[:port]`, defaulting to `result`. External Items enter through `read_item@1.0` with `params.uri` and `params.view`. Connections have no `view` field. Reject dataset-only bindings, missing required ports, duplicate bindings, unsupported representations, and schema mismatches explicitly. Do not silently select all dataset items, merge their contents, or invoke the operator repeatedly. A shell item number resolves to a specific path in the current listing before submission.

Dataset-wide work belongs to job orchestration:

- Map explicitly selected compatible items through independent operator invocations.
- Combine explicitly selected items by binding each to a declared input port, for example `left` and `right` for a join.

Map must be requested explicitly; it is not inferred from a dataset selection. Its selection, concurrency, ordering, and failure policy need a documented contract before implementation. Multi-item aggregation requires an explicitly designed input representation or port contract rather than an arbitrary dataset parameter.

Outputs follow the same item-level rule. Each named output is an item stream with metadata and content; several output ports do not automatically form a saved dataset. Jobs connect output ports to downstream input ports. Dataset item paths identify sources; output port names identify in-flight results and do not allocate published dataset names.

## Display and lifetime

A stream carries an output name and explicit content metadata: representation, format, and schema where applicable. Records can be shown as tables, text as text, and unrecognized binary content as incremental hex lines. Arrays, images, and grouped scientific objects must preserve their structures through explicitly documented representations; do not silently flatten them into records.

`run` is synchronous from the shell user's perspective: after submission, the shell waits for stream metadata and prints content incrementally as batches or chunks arrive, without collecting the full result first. The prompt returns only after all output streams complete successfully, execution fails, or the user interrupts. Waiting for the first result is distinct from successful completion; a stalled request remains subject to its execution timeout.

Records display column headers once per output and append rows as batches arrive. Text decoding preserves characters across chunk boundaries and writes text incrementally. Binary results use 16-byte hex lines with per-port offsets. Label multiple output streams with their port names and metadata so their content can be distinguished; the Flight envelope carries explicit port labels. Keep display buffering bounded.

The default consumes and displays the complete stream. A preview limit must be requested explicitly; it is not applied silently to `run`. Ctrl-C cancels the request and its upstream execution, releases resources, and returns to the prompt. Existing browsing preview defaults do not imply a limit for operator execution.

The shell displays results as they arrive. A stream is consumed once; it is not a browsable dataset and cannot be reopened after consumption. Show completion only after successful operator exit and valid stream termination. If an operator fails after emitting data, report that previously displayed data is partial.

A bounded preview stops consumption and cancels the remaining request rather than continuing computation invisibly. Client disconnects and interruption must propagate cancellation through producers, upstream Flight reads, and operator containers. Slow consumers apply backpressure with bounded buffers. Named outputs share one framed exchange and may interleave.

## Internal temporary storage

No user-visible output file or persisted result dataset is created. Operators or formats may still need bounded temporary scratch storage, for example to finish Parquet metadata. Scratch storage is an implementation detail, cleaned on completion, failure, or cancellation; it is not an alternate result destination. Stream-only delivery does not imply that every format can emit useful bytes immediately.

## Implemented transport and scheduling limits

`OperatorRequest` selects job YAML or an internal task JSON request over Flight DoExchange. Each batch has one binary `data` cell containing a frame: big-endian 32-bit header length, big-endian 32-bit payload length, JSON header, then payload. Headers are limited to 64 KiB and payloads to 8 MiB. Events are `start`, `data`, `end`, and final `complete`. Records starts include an IPC schema; each records data frame contains one independent Arrow IPC stream with exactly one batch. Bytes frames preserve bytes unchanged. Completion follows successful execution and valid termination of every exposed port.

Operators receive named inputs through Arrow IPC FIFOs and write frames to stdout. Stderr carries diagnostics. Built-in operators and external executor plugins use the same contracts. Output datasets, output TTLs, legacy output directories, and result-mode selectors have been removed.

Each output has at most one consumer. Multiple input ports must be consumed in lexical order. General pipeline scheduling, fan-out, dataset mapping, retries, checkpoints, and replay are not implemented. New representations require explicit contracts; bytes and records are implemented.

## Shell preview controls

`run ... --preview-rows 20` bounds records per port; `run ... --preview-bytes 4096` bounds bytes per port. `submit pipeline.yaml` accepts the same flags. The first reached limit cancels the entire remaining job, reports the affected port, and does not report successful completion. Without these flags, every result is displayed. Browsing limits do not affect jobs.

## SDK resource ownership

Python `Job.run` returns a context-managed `JobStream`. Scala `Job.run` returns an `AutoCloseable` iterator of `OutputEvent`; use `Using.resource`. Both release transport resources on exhaustion or failure; explicitly close on early exit to cancel execution. Scala event byte arrays remain valid after advancing or closing; close record readers created from those arrays separately. See the [Scala SDK](../dacp-client-scala/README_en.md) and [inline-source examples](operator-source_en.md).
