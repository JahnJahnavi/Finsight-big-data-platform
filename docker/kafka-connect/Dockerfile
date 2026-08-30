# FinSight - Kafka Connect image
# Extends the Confluent Kafka Connect image with the HDFS 3 Sink connector, which
# is used from Phase 2 onwards to land the `txn-raw` topic into HDFS as Parquet.
# Installing it at build time keeps `docker compose up` fully offline-capable.
FROM confluentinc/cp-kafka-connect:7.5.0

ARG HDFS_SINK_VERSION=1.1.26

# confluent-hub is bundled in the base image.
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs3:${HDFS_SINK_VERSION}

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
