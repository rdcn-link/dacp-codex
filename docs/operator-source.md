# DEBUG source operators

[中文](operator-source_zh.md)

DACP accepts inline Python or Scala source only when both the coordinator and execution nodes use `mode: DEBUG`. Ordinary repository operators remain available in NORMAL mode. Source execution is intended for trusted developers with job-execution permission; it does not bypass authentication or runtime restrictions.

## Configure runtimes

```yaml
mode: DEBUG
source_runtimes:
  python@1.0: {executor: python, image: 'xstream/operator-python:1.0'}
  scala@1.0: {executor: jvm, image: 'xstream/operator-scala-debug:1.0'}
```

The administrator installs these images on each execution node. Submitted source cannot choose arbitrary images or install dependencies. Executors compile and run source within the configured isolation and resource limits. See the [XStream executor guide](https://github.com/rdcn-link/xstream-codex/blob/main/plugins/xstream-plugin-operator-executors/README.md) and [operator development guide](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring.md).

## Submit from the shell

```text
run --runtime python@1.0 --input source=/orders --params '{"output_format":"csv"}' <<END
def process(inputs, params):
    for chunk in inputs['source']:
        yield chunk.upper()
END
```

Source entry always uses a heredoc. The delimiter must appear alone on its closing line. Source is limited to 512 KiB and the complete job request to 1 MiB. `--view records` selects record inputs; Python input iterators yield PyArrow record batches instead of byte chunks. Yield the corresponding output chunks through the `result` port. Send diagnostics to stderr.

For Python/Scala implementation, SDK framing and launcher interfaces, use the [XStream operator guide](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring.md) and its [inline source examples](https://github.com/rdcn-link/xstream-codex/blob/main/docs/local-operators.md).

The shell receives output incrementally. Ctrl-C cancels execution. Preview limits are available through `--preview-rows` and `--preview-bytes`. No result Dataset is created. Source and ordinary repository operators are mutually exclusive choices for one step.