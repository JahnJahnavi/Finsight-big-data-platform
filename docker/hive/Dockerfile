# FinSight - Hive image
#
# Apache Hive 3.1.3 (NOT 4.x): Spark 3.5's Hive metastore client only supports
# metastore versions up to 3.1.x. A Hive 4.0.1 metastore fails with
# "Invalid method name: 'get_table'" when Spark SQL connects. See docs/ASSUMPTIONS.md I2.
#
# Extends the base image with the PostgreSQL JDBC driver required to use
# PostgreSQL as the metastore backend (the base ships only the Derby driver and
# has neither curl nor wget). The driver is fetched in a throwaway Alpine stage.

ARG HIVE_VERSION=3.1.3
ARG PG_JDBC_VERSION=42.7.4

FROM alpine:3.20 AS fetch
ARG PG_JDBC_VERSION
RUN wget -O /postgresql.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"

FROM apache/hive:3.1.3
ARG PG_JDBC_VERSION
USER root
COPY --from=fetch /postgresql.jar /opt/hive/lib/postgresql-${PG_JDBC_VERSION}.jar
RUN chown hive:hive /opt/hive/lib/postgresql-${PG_JDBC_VERSION}.jar
USER hive
