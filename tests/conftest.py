"""This file configures pytest.

This file is in the root since it can be used for tests in any place in this
project, including tests under resources/.
"""

import csv
import json
import os
import pathlib
import sys
from contextlib import contextmanager
from typing import Callable, Generator

import pytest
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture()
def spark() -> SparkSession:
    """Provide a SparkSession fixture for tests.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture()
def load_fixture(spark: SparkSession) -> Callable[[str], DataFrame]:
    """Provide a callable to load JSON or CSV from fixtures/ directory.

    Example usage:

        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """

    def _loader(filename: str) -> DataFrame:
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)  # pyright: ignore[reportUnknownMemberType]
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)  # pyright: ignore[reportUnknownMemberType]
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest session."""
    with _allow_stderr_output(config):
        _enable_fallback_compute()

        # Initialize Spark session eagerly, so it is available even when
        # SparkSession.builder.getOrCreate() is used. For DB Connect 15+,
        # we validate version compatibility with the remote cluster.
        if hasattr(DatabricksSession.builder, "validateSession"):
            DatabricksSession.builder.validateSession().getOrCreate()
        else:
            DatabricksSession.builder.getOrCreate()


@contextmanager
def _allow_stderr_output(config: pytest.Config) -> Generator[None, None, None]:
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def _enable_fallback_compute() -> None:
    """Enable serverless compute if no compute is specified."""
    conf = WorkspaceClient().config
    if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
        return

    url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
    print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)  # noqa: T201
    print(f"  see {url} for manual configuration", file=sys.stdout)  # noqa: T201

    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"
