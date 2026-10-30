# Scientific views in XStream Designer

The designer browses local and configured remote DataCatalogs. Expand a Dataset, a file container and its children to choose a logical Item: a NetCDF variable, a FITS HDU or FASTA `/sequences`. Capability symbols distinguish records, bytes, arrays, images and sequences. Folders expand in place; their original bytes remain available through explicit `read_item` parameters.

Selecting an Item creates or updates a zero-input `read_item@1.0` step. The automatic preference is records, then bytes, then the first advertised scientific view. Choose another available representation in **Parameters → view**. An explicit unsupported view fails. The selection form comes from the installed view codec, independently of the file format; new codecs can supply their own form without changing the designer's registry.

## Selection and variables

Enable **selection**, then enable the individual fields to restrict a scientific read. Omit selection to read the complete object. Switching view clears its old selection. Parameters support the same `$VARIABLE` values as other Job parameters; provide their values on the Variables tab. Export preserves variables and selections.

| View | Selection fields | Structure |
|---|---|---|
| array | `origin`, `shape`, `stride`, all in declared axis order; stride defaults to 1 | Dimension names/lengths, coordinate references, units, numeric encoding and attributes |
| image | `x`, `y`, `width`, `height` | Full image width/height, channels, encoding and attributes |
| sequence | optional `ids`, `offset`, `length`; offsets count symbols within each sequence | Alphabet and attributes; each data block retains sequence ID, offset and final marker |

```yaml
name: Read a climate slice
tasks:
  - id: read
    operator: read_item@1.0
    inputs: {}
    params:
      uri: context:/science/climate/temperature
      view: array
      selection:
        origin: [0, 0]
        shape: [$TIME_COUNT, 180]
        stride: [1, 1]
outputs:
  result: read
```

Variables YAML: `TIME_COUNT: 4`. This example assumes `/science/climate/temperature` is a two-dimensional variable with compatible bounds. For a FITS image, use `uri: context:/science/sky/hdu0`, `view: image`, and `selection: {x: 10, y: 20, width: 32, height: 32}`. For FASTA, use `uri: context:/science/genome/sequences`, `view: sequence`, and `selection: {ids: [chr1], offset: 100, length: 80}`. These file aliases must be published in the execution catalog. Explicit remote sources use `dacp://host:port/dataset/item`. SQL is exclusively for records and cannot be combined with scientific selection.

## Structure and samples

Step cards display compact structure tables. The right **Outputs** tab shows the readable structure, complete descriptor JSON and samples in a table. Array/image sample rows are blocks containing coordinates and a bounded list of raw values; they are not a flattened records view. Sequence rows include ID, offset, symbols and final marker. `values_truncated` / `symbols_truncated` identify shortened block contents; sample `truncated` also indicates row/byte limits or early termination. Image samples show numeric tile values, not a rendered thumbnail.

Inference follows upstream dependencies and updates changed steps and their descendants. Changing a source selection is a parameter change and triggers fresh inference; moving cards or removing downstream steps does not invalidate a retained upstream result. Scientific source inference reads a descriptor directly from the typed view and does not launch an operator worker. Samples are read only when requested, within 10 rows/16 KiB per port by default, 100 rows/64 KiB maximum and the existing shared response budget. The reader closes after reaching the budget or cancellation. Metadata-only requests stop after the descriptor.

## Execution and extensions

`read_item` carries the selection into local and remote reads, including Union member reads. Scientific Job frames use the original representation name, `start.structure` for the versioned codec descriptor and `data.block` for per-block coordinates/identity; the data payload remains the codec's binary block. They can connect to Outlet. Downstream operators must explicitly support that representation; byte/record templates do not automatically consume scientific views. The designer's compatibility validation reports mismatches. Scala Job output events retain `structure` and `block`; Python events retain these header fields.

Implement a `DataViewCodec` and register it under `META-INF/services/xstream.data.DataViewCodec`. Override `selectionSchema` with the typed form declaration, `validateSelection` for descriptor-dependent bounds and `preview(block, maxValues)` for an object representing a bounded sample. The host applies the final row/byte budget; preview code must honor `maxValues` and avoid whole-object reads. Format-specific dependencies and behavior belong to format plugins. See [data stream contracts](data-streams.md) and [xstream-data](../xstream-data/README.md) for scientific format support and random-access staging limits.

Small synthetic fixtures are provided in `examples/data/scientific`: `climate.nc`, `sky.fits` and `genome.fa`. Load [scientific-selection.yaml](../examples/xstream/scientific-selection.yaml); it assumes the `local_files` Dataset publishes `examples/data`.
