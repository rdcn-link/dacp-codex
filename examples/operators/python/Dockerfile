# Build a shared Python/Arrow environment; publish its image ID through the repository.
FROM python:3.12-slim
RUN python -m pip install --no-cache-dir pyarrow==23.0.1

COPY dacp-operator-sdk/python/dacp_operator /opt/dacp-sdk/dacp_operator
ENV PYTHONPATH=/opt/dacp-sdk
