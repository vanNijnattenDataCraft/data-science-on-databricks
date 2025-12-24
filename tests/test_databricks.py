import pytest


@pytest.mark.databricks
@pytest.mark.parametrize("to_pass", [(True), (True)])
def test_passes(to_pass: bool) -> None:
    assert to_pass
