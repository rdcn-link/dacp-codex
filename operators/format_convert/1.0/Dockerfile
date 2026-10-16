FROM eclipse-temurin:17-jdk AS build
WORKDIR /src
COPY dacp-server /src/dacp-server
COPY proto /src/proto
RUN curl -fsSL https://raw.githubusercontent.com/paulp/sbt-extras/master/sbt -o /usr/local/bin/sbt && chmod +x /usr/local/bin/sbt
WORKDIR /src/dacp-server
RUN sbt stage
FROM eclipse-temurin:17-jre
COPY --from=build /src/dacp-server/target/universal/stage /opt/dacp
ENTRYPOINT ["/opt/dacp/bin/dacp-server"]
