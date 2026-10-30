# Typed data views and streams

A DataCatalog contains Datasets; each Dataset exposes logical Items. An Item may be browsable, readable, or both. A file container can expose original bytes and children simultaneously. Its children may be groups, tables, variables, image HDUs or sequences. Folder status alone does not determine readability.

## Contracts

`DatasetAccess.view(path, DataViewType[K,S])` discovers an optional `DataView[K,S]`. Opening a view with a typed selection `S` and `StreamContext` returns a single-use `DataStream[K]`. `consume` synchronously calls `start(descriptor)` once, then `accept(chunk)` zero or more times. Successful return means completion; an exception means failure. Empty streams still announce their descriptor. Consumption closes the stream; callers should also close it in `finally`.

Chunks are bounded blocks, not whole files or individual rows. The consumer returns before the producer reads another block: this supplies backpressure without a per-stream thread. Arrow roots, dictionaries, byte arrays and buffers are borrowed during the callback. Copy explicitly to retain them. Allocators remain caller-owned. Implementations acquire cursors and file handles inside consumption and release them in `finally`, including after a consumer exception. Cancellation is cooperative through `context.check`; implementations call it before reads and emissions. Closing an unused stream acquires no resources. Closing a consuming stream prevents subsequent callbacks; it does not promise to interrupt arbitrary blocking third-party I/O.

| View token | Block | Selection | Descriptor |
|---|---|---|---|
| `DataViews.Bytes` | `ByteChunk` with offset/length | `AllBytes` | media type |
| `DataViews.Records` | `RecordBatch` with borrowed Arrow root and dictionaries | `Scan` | Arrow schema and dictionaries |
| `ScientificViews.Arrays` | `ArrayChunk` | `ArraySelection` | dimensions, coordinates, numeric encoding and attributes |
| `ScientificViews.Images` | `ImageTile` | `ImageSelection` | image dimensions, channels, encoding and attributes |
| `ScientificViews.Sequences` | `SequenceChunk` | `SequenceSelection` | alphabet and attributes |

NetCDF, FITS and FASTA parsers and scientific view network codecs are implemented; see [xstream-data](../xstream-data/README.md). The built-in read_item source can emit scientific Job frames; downstream operators must explicitly support the chosen representation. Python clients expose descriptor/payload blocks.

A third-party plugin may define its own `DataViewType[K,S]` and `DataDescriptor[K]`. Share the same token instance between producer and consumer. Discovery uses token identity, not unchecked matching of strings to unrelated block types. Item metadata lists representation names; that advertisement does not install a transport codec.

## Reading records

```scala
val view = access.requireView("/observations", DataViews.Records)
val stream = view.open(Scan(), StreamContext(allocator, () => cancellation.check()))
try stream.consume(new DataConsumer[RecordBatch] {
  def start(d: DataDescriptor[RecordBatch]): Unit =
    println(d.asInstanceOf[RecordsDescriptor].schema)
  def accept(batch: RecordBatch): Unit =
    println(batch.root.getRowCount) // consume before returning
}) finally stream.close()
```

Use a `Scan` for projection, filtering and a global row limit. JDBC and Parquet retain their source optimizations; other readers use bounded filtering. A plain records read preserves dictionaries; scans on dictionary-encoded records remain explicitly unsupported. `BatchDatasetAccess` / `BatchItemAccess` are optional Arrow producer utilities, using `BatchReceiver`, never a Flight listener. New implementations can implement `DatasetAccess` / `ItemAccess` directly and emit typed blocks.

Original file bytes take precedence. SQL tables expose Arrow IPC bytes. When an Item supports only records, `ItemByteViews` derives bounded UTF-8 JSONL bytes (`application/x-ndjson`), preserving nested values and decoding top-level dictionaries. A byte chunk need not align with a row or UTF-8 character. Errors never flush buffered partial JSON. Transports encode streams at their boundary; local execution and S3 consume typed blocks directly. Neither this abstraction nor a view conversion implicitly materializes output.

## Scientific mappings

| Format | Browsing and native views | Rules |
|---|---|---|
| CSV / JSONL | file Item: records and original bytes | batches retain schema, nulls and nested values |
| NetCDF | groups and variables as child Items; variable: array | preserve rank, dimension names, coordinate references, units, fill values and attributes; select by origin/shape/stride |
| FITS | HDUs as child Items; table HDU: records; image HDU: array or image | preserve HDU identity, headers, axes, scaling and coordinate metadata; no automatic flattening |
| FASTA | file bytes; sequence view, optionally sequence children | chunks identify sequence ID and symbol offset and mark final chunk; never require a complete chromosome in memory |
| JPEG | original bytes, optional decoded image view | preserve image dimensions and channels; decoding is explicit |

Scientific numeric payloads use an explicit primitive encoding and byte order; their buffers hold dense row-major chunk values. Dimensions and chunk origins are separate from payload bytes. Coordinate Items are referenced by logical path rather than copied into every chunk. Format plugins validate selection bounds, scaling and format-specific attributes. Unsupported capabilities are absent, not empty streams.

## Contributing a reader

Register a `DatasetProvider` or `FormatHandler` / `StructuredFileHandler` through `META-INF/services` as described in [Plugins](plugins.md). Dependencies belong to the plugin module. A structured handler returns a DatasetAccess over its internal logical objects; it must not expose storage paths as logical paths. Configure provider-owned settings under the dataset `source` / item settings; for example:

```yaml
datasets:
  climate:
    provider: my-netcdf
    source:
      root: /srv/climate
```

`my-netcdf` is an example external provider, not a bundled provider. Its plugin owns and validates `root`, variable selection and decoding options. New view consumers require no core registry changes for in-process access. New wire representations require a separately specified codec and transport capability negotiation.

Contract tests must cover empty descriptors, bounded chunks, borrowed-buffer lifetimes, selection and limits, resource release on consumer failure/cancellation, missing views, nested browsing, and exact scientific dimension/coordinate/sequence preservation.

## Scientific network representation (version 1)

Scientific views use a stream of bounded blocks, with no whole-file serialization. The request ticket contains the view name and UTF-8 JSON selection. Arrays use `{origin,shape,stride}` (omitted means the complete variable), images use `{x,y,width,height}` (omitted means the complete image), and sequences use `{ids,offset,length}` with optional fields. Records scans and scientific selections cannot be combined.

The response is an Arrow envelope with `metadata: utf8` and `data: binary`, one row per block. Schema metadata `xstream.view` identifies the representation and `xstream.descriptor` contains its JSON descriptor. A descriptor is available even for empty streams. Each block has JSON coordinates/identity plus raw payload bytes. Array/image numeric buffers use the descriptor's byte order and dense row-major values; sequence payloads are UTF-8 symbols with offsets counted in symbols. Metadata is bounded to 1 MiB and blocks to 16 MiB. Network and local readers propagate failure/cancellation and do not implicitly materialize data.

`DataViewCodec` is discovered through ServiceLoader, independently of format plugins. It owns typed selection validation, descriptors, block encoding and decoding. Services and generic clients dispatch through registered codecs; adding another codec does not require editing a core registry. NetCDF/FITS/FASTA readers and the standard scientific codecs are distributed as separate xstream-data plugins. Array and image requests use `ArraySelection` / `ImageSelection`, whose absent section/region selects the whole object; returned chunks always have concrete coordinates.

[Designer scientific views and Job frame contract](designer-scientific.md).
