# xstream-data

Typed data access for XStream and DACP, independent of the job engine and network services. Public contracts live in `xstream.data`; implementations are separate ServiceLoader plugins. See [typed streams](../docs/data-streams.md) and [plugin development](../docs/plugins.md).

## Modules

- `xstream-data-core`: DataCatalog → Dataset → Item, DataView/DataStream, selections, plugin lifecycle and codec contracts.
- `xstream-data-files`, `xstream-data-catalog-static`, `xstream-data-mounted`: file/JDBC, configured and mounted catalogs.
- `xstream-data-formats-basic`, `xstream-data-parquet`, `xstream-data-excel`, `xstream-data-archives`: existing readers and structured browsing.
- `xstream-data-netcdf`: NetCDF groups and numeric variables, bounded strided array selections. Non-numeric variables remain browsable without an array view.
- `xstream-data-fits`: HDUs, bounded image/array tiles and supported fixed-size binary tables; variable-size and complex table cells are not advertised as records.
- `xstream-data-fasta`: bounded sequence chunks with IDs, symbol offsets and final markers; ID/range selection scans the source sequentially.
- `xstream-data-scientific-codecs`: version 1 array/image/sequence descriptors and block codecs for network adapters.

## Build and use

```bash
cd xstream-data
sbt compile
```

Applications depend on core at compile time and selected implementations at runtime. DACP server distributions bundle the scientific plugins. Register formats in `META-INF/services/xstream.data.FormatHandler` and codecs in `META-INF/services/xstream.data.DataViewCodec`; entries contain fully qualified implementation class names. External JARs and their dependencies can use the existing plugin directory; core registries need no edits.

Configure a file item with `format: netcdf`, `format: fits` or `format: fasta`. NetCDF/FITS accept `chunk_bytes` (8–16777216, default 65536). FASTA accepts `chunk_symbols` (1–1048576, default 65536) and `alphabet` (`dna`, `rna`, `protein`, `unknown`). Format parameters belong to the item's `options` map.

NetCDF children use group/variable paths; FITS children use `/hdu0`, `/hdu1`, etc.; FASTA exposes `/sequences`. The file root retains original bytes. Numeric values retain raw encoding and scaling/null metadata. No implicit scientific-to-records flattening occurs. NetCDF/FITS libraries require local random access: remote ByteSources are explicitly staged to a temporary file and deleted on access close; local files are not copied. NetCDF-4/HDF5 support depends on the installed NetCDF backend and is not promised by the bundled pure Java reader. Compressed FITS images and unsupported column types do not imply supported scientific views.

Scientific network adapters preserve typed descriptors and bounded blocks through nodes, Union proxies and clients. read_item supports scientific selections; the designer provides structure and sample previews. Downstream operators must explicitly support the representation. See [designer scientific views](../docs/designer-scientific.md).

Run parser/codec contract tests with `cd ../dacp-server && sbt 'testOnly dacp.DataStreamSpec dacp.ScientificFormatsSpec'`.

## Configuration example

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
