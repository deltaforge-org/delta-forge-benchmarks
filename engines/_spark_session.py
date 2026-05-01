"""Vendored Spark session factory.

Source: `delta-forge/delta-forge-demos/verify_lib/spark_session.py`, pinned at
the engine repo's `DF_GIT_SHA` recorded in this run's manifest.json.

Why vendored: the bench repo must be self-contained. A reviewer should not
need the engine repo on their machine to read the bench's Spark config. When
the upstream file changes, this copy is updated by hand and the new
`DF_GIT_SHA` is recorded; the diff is the bench repo's record of which Spark
config was tested against which engine commit.

This module is the **stock-default** baseline. The tuned baseline lives in
`spark_tuned_engine.py` and overrides specific keys here.
"""
from __future__ import annotations

import os
import subprocess
import sys


SPARK_VERSION = "4.0.0"
DELTA_VERSION = "4.0.0"


_REQUIRED = {
    "pyspark": f"pyspark=={SPARK_VERSION}",
    "delta": f"delta-spark=={DELTA_VERSION}",
}
for _import_name, _pip_pkg in _REQUIRED.items():
    try:
        __import__(_import_name)
    except ImportError:
        print(f"  Installing missing dependency: {_pip_pkg}")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", _pip_pkg, "-q"],
            stdout=subprocess.DEVNULL,
        )


# Spark 4.0 requires JDK 17+. Auto-detect a usable JAVA_HOME if one is not set.
if not os.environ.get("JAVA_HOME"):
    _JDK_CANDIDATES = [
        os.path.expanduser("~/local/jdk"),
        os.path.expanduser("~/.jdks/temurin-17"),
        os.path.expanduser("~/.jdks/temurin-21"),
        "/opt/java/openjdk",
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-21-openjdk-amd64",
    ]
    for _jh in _JDK_CANDIDATES:
        if os.path.isfile(os.path.join(_jh, "bin", "java")):
            os.environ["JAVA_HOME"] = _jh
            break
    if not os.environ.get("JAVA_HOME"):
        try:
            import jdk  # type: ignore
            _jh = jdk.install("17")
            os.environ["JAVA_HOME"] = _jh
        except Exception:
            print("Error: JAVA_HOME is not set and no JDK 17+ found.")
            print("Spark 4.0 requires JDK 17+.")
            print("Install a JDK, set JAVA_HOME, or: pip install install-jdk")
            sys.exit(1)


_session = None


def get_spark():
    """Return a cached SparkSession with stock-default Delta-on-Spark config.

    Quoted exactly in the README under "Spark configurations / stock-defaults"
    so reviewers can audit without opening source files.
    """
    global _session
    if _session is not None:
        try:
            _session.sparkContext._jsc.sc().isStopped()
            return _session
        except Exception:
            _session = None

    from pyspark.sql import SparkSession

    try:
        from delta import configure_spark_with_delta_pip  # type: ignore
        builder = configure_spark_with_delta_pip(
            SparkSession.builder
                .appName("delta-forge-bench-spark-default")
                .master("local[*]")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.driver.memory", "4g")
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.log.level", "WARN")
        )
        _session = builder.getOrCreate()
    except ImportError:
        _session = (
            SparkSession.builder
            .appName("delta-forge-bench-spark-default")
            .master("local[*]")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "4g")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.log.level", "WARN")
            .getOrCreate()
        )
    return _session


def stop_spark():
    global _session
    if _session is not None:
        try:
            _session.stop()
        except Exception:
            pass
        _session = None
