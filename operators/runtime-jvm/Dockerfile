# Shared dependency environment; operator code is published separately.
FROM scratch
COPY rootfs/ /
ENV JAVA_HOME=/opt/java
ENV JAVA_TOOL_OPTIONS="-Dorg.xerial.snappy.lib.path=/opt/native -DZstdNativePath=/opt/native/libzstd-jni.so"
