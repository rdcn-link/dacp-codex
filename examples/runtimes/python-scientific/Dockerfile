# Build from a pinned Python/Arrow image already loaded locally.
ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS dependencies
COPY target/wheels/ /tmp/wheels/
COPY requirements.txt /tmp/requirements.txt
RUN ["/usr/local/bin/python", "-m", "pip", "install", "--no-index", "--find-links", "/tmp/wheels", "--target", "/opt/science", "-r", "/tmp/requirements.txt"]
FROM ${BASE_IMAGE}
COPY --from=dependencies /opt/science/ /opt/science/
ENV PYTHONPATH=/opt/science:/opt/dacp-sdk:/opt/python/lib/python3.12/site-packages
