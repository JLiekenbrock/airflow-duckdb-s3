import argparse

import pytest

from runner.run_dbt import parse_stichtage


def test_parse_stichtage_accepts_iso_dates():
    assert parse_stichtage('["2026-06-30", "2026-07-31"]') == [
        "2026-06-30",
        "2026-07-31",
    ]


@pytest.mark.parametrize("value", ["[]", '"2026-06-30"', '["2026-02-30"]'])
def test_parse_stichtage_rejects_invalid_values(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_stichtage(value)

