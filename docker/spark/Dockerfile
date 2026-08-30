# FinSight - Spark image
#
# apache/spark:3.5.3 + pandas + pyarrow. The base image ships neither, but
# PySpark's Arrow APIs (pandas_udf, applyInPandas, toPandas) need them and are
# used from Phase 6 onwards (CLV scoring, Alteryx fallback).
#
# pandas is pinned to 1.5.3: PySpark 3.5.x's applyInPandasWithState code path
# calls pandas concat internals that break on pandas 2.x
# ("AttributeError: 'NoneType' object has no attribute 'value'"). Both pins
# support the base image's Python 3.8.
FROM apache/spark:3.5.3

USER root
RUN pip install --no-cache-dir \
      "pandas==1.5.3" \
      "pyarrow==12.0.1"
USER spark
