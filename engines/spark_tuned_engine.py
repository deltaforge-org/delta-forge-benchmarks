"""Spark with a defensible tuning profile (best-known OSS settings).

Every tuning key here has a one-line rationale, because the entire point
of publishing two Spark baselines is that "you misconfigured Spark" is
the most common critique. An un-rationalized tuning is worse than no
tuning, because reviewers can't tell whether you cherry-picked.

Sources for the picks below:
  - Apache Spark 4.0 tuning guide: https://spark.apache.org/docs/4.0.0/tuning.html
  - Spark SQL performance tuning: https://spark.apache.org/docs/4.0.0/sql-performance-tuning.html
  - Delta Lake performance + concurrent writes docs (OSS, not Databricks-only):
    https://docs.delta.io/latest/optimizations-oss.html

Settings excluded on purpose:
  - `spark.databricks.delta.optimizeWrite.enabled` and
    `spark.databricks.delta.autoCompact.enabled` are Databricks-runtime-only
    and silently ignored on OSS Delta. Do not add them.
  - `spark.sql.shuffle.partitions=200` is Spark's default; we size dynamically
    instead so SF=1 doesn't pay the 200-tiny-partitions overhead.

Full tuning rationale table:

| Key                                                    | Value             | Why |
|--------------------------------------------------------|-------------------|-----|
| spark.sql.adaptive.enabled                             | true              | AQE on. Default in 3.2+. Listed explicit so the README shows we kept it. |
| spark.sql.adaptive.coalescePartitions.enabled          | true              | Merges tiny shuffle partitions; biggest single AQE win at small/medium scale. |
| spark.sql.adaptive.skewJoin.enabled                    | true              | Re-partitions skewed join keys mid-run. Important on lineitem joins where a few keys dominate. |
| spark.sql.adaptive.skewJoin.skewedPartitionFactor      | 5                 | A partition is skewed if it's >= 5x the median. Spark default. |
| spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes | 256MB        | Floor for skew detection. Spark default 256MB. |
| spark.sql.adaptive.localShuffleReader.enabled          | true              | Local-shuffle path on broadcast joins; reduces stage count. |
| spark.sql.shuffle.partitions                           | sized at start    | We compute from input bytes (~128MB per partition). Default 200 is wrong for SF<=10. |
| spark.sql.autoBroadcastJoinThreshold                   | 100MB             | Default 10MB is too low; 100MB lets dim tables broadcast naturally on TPC-H. |
| spark.driver.memory                                    | 8g                | Local mode = driver runs everything. 8g matches a typical reviewer's expectation; bench container budget is 16g (default). |
| spark.executor.memory                                  | 8g                | Same JVM in local mode, but Spark reads both keys; setting both makes the resolved config unambiguous. |
| spark.memory.fraction                                  | 0.7               | Up from default 0.6: more for execution + storage, less for user objects. Helps shuffle-heavy plans. |
| spark.memory.storageFraction                           | 0.3               | Down from 0.5: TPC-H is execution-heavy (joins/aggs), not cache-heavy. |
| spark.memory.offHeap.enabled                          | true              | Off-heap memory bypasses JVM GC pressure for shuffle buffers. |
| spark.memory.offHeap.size                              | 4g                | Headroom over the 8g on-heap. Keeps GC pauses low under shuffle. |
| spark.sql.execution.arrow.pyspark.enabled              | true              | Arrow path for Python<->JVM exchange; faster collect() that the harness uses. |
| spark.sql.parquet.compression.codec                    | snappy            | Spark default; listed for parity. |
| spark.sql.parquet.enableVectorizedReader               | true              | Spark default; listed for parity. |
| spark.sql.parquet.filterPushdown                       | true              | Spark default; listed for parity. |
| spark.sql.parquet.aggregatePushdown                    | true              | Push COUNT/MIN/MAX into Parquet. Helps SF<=10 reads materially. |
| spark.sql.cbo.enabled                                  | true              | Cost-based optimizer; needs ANALYZE TABLE COMPUTE STATISTICS to fire. |
| spark.sql.cbo.joinReorder.enabled                      | true              | Reorder joins by computed stats. Helps multi-join TPC-H queries (Q5, Q7, Q8, Q9). |
| spark.sql.statistics.histogram.enabled                 | true              | Histograms for join selectivity. CBO uses them when available. |
| spark.serializer                                       | org.apache.spark.serializer.KryoSerializer | Kryo is the recommended serializer for performance. |

If you think a tuning here is wrong, open a PR. PRs that add a knob and
a one-line rationale are welcome; PRs that just bump a number without
rationale will be closed.

Note on JDK GC: JDK 17 defaults to G1GC, which is the right choice for
Spark's allocation pattern. We do not set `spark.driver.extraJavaOptions=-XX:+UseG1GC`
explicitly because doing so requires also re-specifying the entire JVM
opts string Spark builds internally. Verifiable via `jcmd <pid> VM.flags`
during a run.
"""
from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Any

from . import _spark_session
from .spark_default_engine import SparkDefaultEngine


def _size_shuffle_partitions(input_bytes: int) -> int:
    """Target ~128 MB of input per shuffle partition, clamped to [8, 200].
    SF=1 (~1 GB) -> ~8 partitions; SF=10 (~10 GB) -> ~80; SF=100 -> 200.
    The 128 MB target matches HDFS / cloud-blob default block sizes, which
    is what AQE's coalesce step naturally aligns to."""
    target_bytes = 128 * 1024 * 1024
    n = max(1, math.ceil(input_bytes / target_bytes))
    return max(8, min(200, n))


class SparkTunedEngine(SparkDefaultEngine):
    """Same lifecycle as SparkDefaultEngine; different builder config."""

    name = "spark-tuned"

    def __init__(self, input_bytes_hint: int | None = None) -> None:
        super().__init__()
        self._chosen_partitions = (
            _size_shuffle_partitions(input_bytes_hint) if input_bytes_hint else 64
        )
        self._config_keys = {
            **SparkDefaultEngine._config_keys,

            # ----- AQE: adaptive query execution -----
            "spark.sql.adaptive.enabled":                                  "true",
            "spark.sql.adaptive.coalescePartitions.enabled":               "true",
            "spark.sql.adaptive.skewJoin.enabled":                         "true",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor":           "5",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
            "spark.sql.adaptive.localShuffleReader.enabled":               "true",

            # ----- partitioning + join thresholds -----
            "spark.sql.shuffle.partitions":          str(self._chosen_partitions),
            "spark.sql.autoBroadcastJoinThreshold":  str(100 * 1024 * 1024),

            # ----- memory: more for execution, less for user objects -----
            "spark.driver.memory":          "8g",
            "spark.executor.memory":        "8g",
            "spark.memory.fraction":        "0.7",
            "spark.memory.storageFraction": "0.3",
            "spark.memory.offHeap.enabled": "true",
            "spark.memory.offHeap.size":    "4g",

            # ----- IO: Parquet -----
            "spark.sql.parquet.compression.codec":     "snappy",
            "spark.sql.parquet.enableVectorizedReader": "true",
            "spark.sql.parquet.filterPushdown":         "true",
            "spark.sql.parquet.aggregatePushdown":      "true",

            # ----- cost-based optimizer + stats -----
            "spark.sql.cbo.enabled":                "true",
            "spark.sql.cbo.joinReorder.enabled":    "true",
            "spark.sql.statistics.histogram.enabled": "true",

            # ----- serialization: Kryo (Spark's official perf recommendation) -----
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",

            # ----- Python<->JVM data path -----
            "spark.sql.execution.arrow.pyspark.enabled": "true",
        }

    def _build_session(self):
        # The vendored get_spark() returns a cached default session. We must
        # build our own session with the tuned overrides. PySpark's builder
        # is a singleton, so we stop any default session first. The runner
        # enforces "one engine at a time"; this is a belt-and-braces guard.
        _spark_session.stop_spark()

        from pyspark.sql import SparkSession

        try:
            from delta import configure_spark_with_delta_pip  # type: ignore
            builder = configure_spark_with_delta_pip(
                SparkSession.builder.appName("delta-forge-bench-spark-tuned")
            )
        except ImportError:
            builder = SparkSession.builder.appName("delta-forge-bench-spark-tuned")

        for k, v in self._config_keys.items():
            if k == "spark.master":
                builder = builder.master(v)
            else:
                builder = builder.config(k, v)

        sess = builder.getOrCreate()
        # Cache it inside _spark_session so stop() at teardown closes the
        # right object.
        _spark_session._session = sess  # type: ignore[attr-defined]
        return sess

    def version_info(self) -> dict[str, Any]:
        info = super().version_info()
        info["chosen_shuffle_partitions"] = self._chosen_partitions
        info["sizing_heuristic"] = (
            "shuffle_partitions = max(8, min(200, ceil(input_bytes / 128MB)))"
        )
        info["tuning_profile_summary"] = (
            "AQE+skew+localShuffleReader; 100MB broadcast threshold; "
            "memory.fraction=0.7 + 4g off-heap; CBO+joinReorder+histograms; "
            "Kryo serializer; Arrow pyspark"
        )
        return info
