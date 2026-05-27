# ─────────────────────────────────────────────────────────────
#  Flink 1.20.4 + Iceberg 1.9.1 runtime
#  Base:  flink:1.20.4-scala_2.12-java11
#  JARs downloaded from Maven Central at build time so the image
#  is fully self-contained and reproducible.
# ─────────────────────────────────────────────────────────────
FROM flink:1.20.4-scala_2.12-java11

ARG MAVEN=https://repo1.maven.org/maven2
ARG ICEBERG_VERSION=1.9.1
ARG FLINK_VERSION=1.20.4
ARG HADOOP_VERSION=3.3.6

# ── Iceberg runtime jar for Flink 1.20 ───────────────────────
RUN wget -q \
    "${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar" \
    -O /opt/flink/lib/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar

# ── Flink S3 Hadoop filesystem plugin (enables s3a:// paths) ─
RUN mkdir -p /opt/flink/plugins/s3-hadoop && \
    wget -q \
    "${MAVEN}/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar" \
    -O /opt/flink/plugins/s3-hadoop/flink-s3-fs-hadoop-${FLINK_VERSION}.jar

# ── Hadoop common (required by Iceberg Hadoop catalog) ────────
RUN wget -q \
    "${MAVEN}/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar" \
    -O /opt/flink/lib/hadoop-common-${HADOOP_VERSION}.jar

# ── Hive SQL connector (Flink 1.20 ships hive 3.1.3 build) ───
RUN wget -q \
    "${MAVEN}/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/${FLINK_VERSION}/flink-sql-connector-hive-3.1.3_2.12-${FLINK_VERSION}.jar" \
    -O /opt/flink/lib/flink-sql-connector-hive-3.1.3_2.12-${FLINK_VERSION}.jar

# Copy SQL jobs and custom config
COPY sql-jobs/ /opt/flink/sql-jobs/
COPY conf/flink-conf.yaml /opt/flink/conf/flink-conf.yaml

WORKDIR /opt/flink
