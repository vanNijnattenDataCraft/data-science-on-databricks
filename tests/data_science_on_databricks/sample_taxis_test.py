import pytest

from data_science_on_databricks import taxis


@pytest.mark.databricks
def test_find_all_taxis() -> None:
    results = taxis.find_all_taxis()
    assert results.count() > 5
