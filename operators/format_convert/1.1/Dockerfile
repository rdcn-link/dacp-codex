# Optional complete image. Its runtime layer is shared with thin-JAR operators.
ARG RUNTIME_IMAGE=dacp/operator-jvm:1.1
FROM ${RUNTIME_IMAGE}
COPY operator.jar /opt/lib/operator.jar
ENTRYPOINT ["/opt/java/bin/java", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true", "-cp", "/opt/lib/*", "dacp.OperatorMain"]
