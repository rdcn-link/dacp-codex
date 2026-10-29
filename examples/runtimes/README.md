# Common operator runtimes

The repository lists approved execution environments at `/runtimes`; developers select the exact `name@version` in their source submission. Runtime archives are shared between operators. Each environment pins both its execution image and its source-build image by SHA-256. These examples target Linux x86-64.

| Runtime | Main dependencies | Source languages | Purpose |
| --- | --- | --- | --- |
| `python-arrow@1.0` | Python 3.12.3, PyArrow 23.0.1 | Python | Arrow record streams, CSV and SQLite analysis |
| `jvm-arrow@1.0` | OpenJDK 17.0.20.1, Scala 2.13.16, Arrow 18.3.0 | Java, Scala | JVM operators sharing Arrow and the DACP SDK |
| `python-scientific@1.0` | Python 3.12.3, PyArrow 23.0.1, NumPy 2.2.6, Pandas 2.2.3, SciPy 1.15.3 | Python | Numerical analysis and scientific record transformations |

The JVM execution image is `dacp/operator-jvm:1.1`; its separate build image is `dacp/operator-scala-debug:1.0`, prepared using [`build-scala-source-runtime.sh`](../../scripts/build-scala-source-runtime.sh). Java and Scala share one catalog entry because they use the same execution dependencies. Python environments use the same image for build and execution.

## Build the scientific Python environment

From the repository root, download the pinned binary wheels using Python 3.12 on Linux x86-64, then build with network access disabled. This avoids relying on certificates or package download tools inside the compact execution image.

```bash
python3.12 -m pip download --only-binary=:all: \
  --dest examples/runtimes/python-scientific/target/wheels \
  -r examples/runtimes/python-scientific/requirements.txt

docker image inspect --format '{{.Id}}' dacp/operator-python:1.0
# Substitute the returned sha256:... image ID for PINNED_BASE_IMAGE.
docker build --network none --build-arg BASE_IMAGE=PINNED_BASE_IMAGE \
  -t dacp/operator-python-scientific:1.0 examples/runtimes/python-scientific

docker run --rm --network none --entrypoint /usr/local/bin/python \
  dacp/operator-python-scientific:1.0 \
  -c 'import numpy, pandas, scipy, pyarrow; print(numpy.__version__, pandas.__version__, scipy.__version__, pyarrow.__version__)'
```

A multi-stage build keeps downloaded wheel archives out of the final image. Scientific dependencies increase the shared runtime size, while individual operator artifacts remain small. Pandas and many SciPy algorithms require bounded data in memory: installing these packages does not make their algorithms streaming.

Register the execution image and build image in the repository administration page, including their pinned IDs, `docker save` archives and dependency versions. For Python choose executor/language `python`; for the JVM environment choose executor `jvm` and build languages `java, scala`. The repository's source builds remain isolated and have no network access. See [source submission and runtime administration](../../docs/operator-submission.md).
