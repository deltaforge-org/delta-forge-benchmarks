"""Spark with a defensible tuning profile (best-known OSS settings).

Every tuning key here has a one-line rationale, because the entire point
of publishing two Spark baselines is that "you misconfigured Spark" is
the most common critique. An un-rationalized tuning is worse than no
tuning, because reviewers can't tell whether you cherry-picked.

Sources for the picks below:
  - Apache Spark 4.0 tuning guide:
    https://spark.apache.org/docs/latest/tuning.html
  - Spark SQL performance tuning (4.0+):
    https://spark.apache.org/docs/latest/sql-performance-tuning.html
  - Spark 4.0 SQL migration guide (what defaults changed):
    https://spark.apache.org/docs/latest/sql-migration-guide.html
  - Delta Lake OSS optimizations:
    https://docs.delta.io/latest/optimizations-oss.html

Settings excluded on purpose:
  - `spark.databricks.delta.optimizeWrite.enabled` and
    `spark.databricks.delta.autoCompact.enabled` are Databricks-runtime-only
    and silently ignored on OSS Delta. Do not add them.
  - `spark.sql.shuffle.partitions=200` is Spark's default; we size dynamically
    instead so SF=1 doesn't pay the 200-tiny-partitions overhead.

Spark 4.0 default-on knobs we explicitly list anyway:
  AQE, DPP, runtime bloom filter, codegen, ANSI mode, vectorized
  parquet reader. Spark flipped these to default-on across 3.2 / 3.3 /
  3.4 / 4.0; we still set them explicitly so reviewers don't have to
  cross-reference release notes to confirm the profile.

Full tuning rationale table:

| Key                                                    | Value             | Why |
|--------------------------------------------------------|-------------------|-----|
| spark.sql.adaptive.enabled                             | true              | AQE on. Default since 3.2. Listed explicit so reviewers can confirm at a glance. |
| spark.sql.adaptive.coalescePartitions.enabled          | true              | Merges tiny shuffle partitions; biggest single AQE win at small/medium scale. |
| spark.sql.adaptive.coalescePartitions.parallelismFirst | false             | Default flipped to true; we prefer fewer-larger partitions at SF<=10 so coalesce isn't capped at the parallelism floor. |
| spark.sql.adaptive.advisoryPartitionSizeInBytes        | 64MB              | Target post-coalesce partition size. Default. |
| spark.sql.adaptive.skewJoin.enabled                    | true              | Re-partitions skewed join keys mid-run. Important on lineitem and cast_info joins where a few keys dominate. |
| spark.sql.adaptive.skewJoin.skewedPartitionFactor      | 5                 | A partition is skewed if it's >= 5x the median. Spark default. |
| spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes | 256MB        | Floor for skew detection. Spark default 256MB. |
| spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled | true       | Spark 3.3+: handles skew during rebalance (DataFrame writes), not just joins. |
| spark.sql.adaptive.localShuffleReader.enabled          | true              | Local-shuffle path on broadcast joins; reduces stage count. |
| spark.sql.adaptive.autoBroadcastJoinThreshold          | 100MB             | AQE-specific broadcast threshold (separate from spark.sql.autoBroadcastJoinThreshold). |
| spark.sql.shuffle.partitions                           | sized at start    | We compute from input bytes (~128MB per partition). Default 200 is wrong for SF<=10. |
| spark.sql.autoBroadcastJoinThreshold                   | 100MB             | Default 10MB is too low; 100MB lets dim tables broadcast naturally on TPC-H/SSB. |
| spark.sql.broadcastTimeout                             | 600               | Default 300s can fire on slow JVM warm-up; doubled for stability. |
| spark.sql.optimizer.dynamicPartitionPruning.enabled    | true              | DPP. Default true. Spark 4.0 added multi-key DPP for compound-partition fact tables. |
| spark.sql.optimizer.dynamicPartitionPruning.useStats   | true              | Use CBO stats (when present) to decide whether to apply DPP. Default true. |
| spark.sql.optimizer.runtime.bloomFilter.enabled        | true              | Runtime bloom filter join. Default since 3.4; helps multi-join TPC-DS / JOB queries materially. |
| spark.sql.optimizer.nestedSchemaPruning.enabled        | true              | Prune unused nested struct fields at read time. Default true; explicit for clarity. |
| spark.sql.cbo.enabled                                  | true              | Cost-based optimizer. CBO falls back to size-based heuristics when ANALYZE TABLE has not been run (which is our case); harmless when stats absent. |
| spark.sql.cbo.joinReorder.enabled                      | true              | Reorder joins by computed stats when available. AQE handles most of this adaptively, but CBO still helps planner choose initial join order. |
| spark.sql.statistics.histogram.enabled                 | true              | Histograms for join selectivity. Used when ANALYZE TABLE ... FOR COLUMNS is run. |
| spark.driver.memory                                    | 8g                | Local mode = driver runs everything. 8g matches a typical reviewer's expectation; bench container budget is 16g. |
| spark.executor.memory                                  | 8g                | Same JVM in local mode, but Spark reads both keys; setting both makes the resolved config unambiguous. |
| spark.memory.fraction                                  | 0.7               | Up from default 0.6: more for execution + storage, less for user objects. Helps shuffle-heavy plans. |
| spark.memory.storageFraction                           | 0.3               | Down from 0.5: analytics queries are execution-heavy (joins/aggs), not cache-heavy. |
| spark.memory.offHeap.enabled                           | true              | Off-heap memory bypasses JVM GC pressure for shuffle buffers. |
| spark.memory.offHeap.size                              | 4g                | Headroom over the 8g on-heap. Keeps GC pauses low under shuffle. |
| spark.sql.files.maxPartitionBytes                      | 128MB             | Default. Listed for parity with the post-coalesce target. |
| spark.sql.files.openCostInBytes                        | 4MB               | File-open cost factored into split planning. Default. |
| spark.sql.inMemoryColumnarStorage.compressed           | true              | Default; compresses cached columnar data. |
| spark.sql.inMemoryColumnarStorage.batchSize            | 10000             | Default; rows per columnar cache batch. |
| spark.sql.codegen.wholeStage                           | true              | Whole-stage code generation. Default; fuses operators into a single Java function. |
| spark.sql.execution.arrow.pyspark.enabled              | true              | Arrow path for Python<->JVM exchange; faster collect() that the harness uses. |
| spark.sql.parquet.compression.codec                    | snappy            | Spark default; listed for parity. |
| spark.sql.parquet.enableVectorizedReader               | true              | Spark default; listed for parity. |
| spark.sql.parquet.filterPushdown                       | true              | Spark default; listed for parity. |
| spark.sql.parquet.aggregatePushdown                    | true              | Push COUNT/MIN/MAX into Parquet. Helps SF<=10 reads materially. |
| spark.sql.ansi.enabled                                 | true              | ANSI SQL semantics. Default in Spark 4.0. Listed for clarity. |
| spark.sql.ansi.doubleQuotedIdentifiers                 | true              | Treat "double-quoted" as identifier (TPC-DS templates do this). Without it, 8 TPC-DS queries fail with ParseException. |
| spark.serializer                                       | KryoSerializer    | Kryo is Spark's recommended performance serializer. |
| spark.kryoserializer.buffer.max                        | 512m              | Default 64m can OOM on wide rows; 512m is the widely-cited safe ceiling. |
| spark.sql.sources.v2.bucketing.enabled                 | true              | Default true in 4.0; enables Storage Partition Join planning for V2 sources. |

If you think a tuning here is wrong, open a PR. PRs that add a knob and
a one-line rationale are welcome; PRs that just bump a number without
rationale will be closed.

Note on ANALYZE TABLE / CBO stats: this profile enables CBO but does not
run `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS` at setup
time, because the workload setup registers Delta tables as temporary
views (not catalog tables, which ANALYZE requires). Without column
stats, CBO falls back to size-based heuristics (file sizes from the
Delta log), which is the same behavior as `cbo.enabled=false` would
produce. AQE handles the runtime adaptation (skew, coalesce, broadcast
swap) that CBO would have done at plan time. We list `cbo.enabled=true`
anyway because it's harmless when stats are absent and reviewers can
add ANALYZE-driven stats by switching the setup to catalog tables if
they want to test the CBO path explicitly.

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
            "spark.sql.adaptive.enabled":                                       "true",
            "spark.sql.adaptive.coalescePartitions.enabled":                    "true",
            "spark.sql.adaptive.coalescePartitions.parallelismFirst":           "false",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes":                  str(64 * 1024 * 1024),
            "spark.sql.adaptive.skewJoin.enabled":                              "true",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor":                "5",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes":      "256MB",
            "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled":    "true",
            "spark.sql.adaptive.localShuffleReader.enabled":                    "true",
            "spark.sql.adaptive.autoBroadcastJoinThreshold":                    str(100 * 1024 * 1024),

            # ----- partitioning + join thresholds -----
            "spark.sql.shuffle.partitions":          str(self._chosen_partitions),
            "spark.sql.autoBroadcastJoinThreshold":  str(100 * 1024 * 1024),
            "spark.sql.broadcastTimeout":            "600",

            # ----- runtime filters (Spark 3.3 / 3.4+ defaults; explicit for review) -----
            "spark.sql.optimizer.dynamicPartitionPruning.enabled":   "true",
            "spark.sql.optimizer.dynamicPartitionPruning.useStats":  "true",
            "spark.sql.optimizer.runtime.bloomFilter.enabled":       "true",
            "spark.sql.optimizer.nestedSchemaPruning.enabled":       "true",

            # ----- memory: more for execution, less for user objects -----
            "spark.driver.memory":          "8g",
            "spark.executor.memory":        "8g",
            "spark.memory.fraction":        "0.7",
            "spark.memory.storageFraction": "0.3",
            "spark.memory.offHeap.enabled": "true",
            "spark.memory.offHeap.size":    "4g",

            # ----- file source split planning -----
            "spark.sql.files.maxPartitionBytes":  str(128 * 1024 * 1024),
            "spark.sql.files.openCostInBytes":    str(4 * 1024 * 1024),

            # ----- columnar in-memory cache -----
            "spark.sql.inMemoryColumnarStorage.compressed": "true",
            "spark.sql.inMemoryColumnarStorage.batchSize":  "10000",

            # ----- codegen -----
            "spark.sql.codegen.wholeStage": "true",

            # ----- IO: Parquet -----
            "spark.sql.parquet.compression.codec":     "snappy",
            "spark.sql.parquet.enableVectorizedReader": "true",
            "spark.sql.parquet.filterPushdown":         "true",
            "spark.sql.parquet.aggregatePushdown":      "true",

            # ----- cost-based optimizer + stats -----
            "spark.sql.cbo.enabled":                  "true",
            "spark.sql.cbo.joinReorder.enabled":      "true",
            "spark.sql.statistics.histogram.enabled": "true",

            # ----- ANSI SQL + identifier compatibility -----
            "spark.sql.ansi.enabled":                  "true",
            "spark.sql.ansi.doubleQuotedIdentifiers":  "true",

            # ----- serialization: Kryo (Spark's official perf recommendation) -----
            "spark.serializer":                "org.apache.spark.serializer.KryoSerializer",
            "spark.kryoserializer.buffer.max": "512m",

            # ----- Python<->JVM data path -----
            "spark.sql.execution.arrow.pyspark.enabled": "true",

            # ----- V2 source bucketing for Storage Partition Join -----
            "spark.sql.sources.v2.bucketing.enabled": "true",
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
            "Spark 4.0 profile: AQE (coalesce+skew+rebalance+localShuffleReader+autoBroadcast 100MB); "
            "DPP+useStats; runtime bloom filter; nested schema pruning; 8g driver+executor + 4g off-heap; "
            "CBO+joinReorder+histograms; ANSI mode + doubleQuotedIdentifiers; "
            "Kryo 512m; Arrow pyspark; V2 bucketing"
        )
        return info
