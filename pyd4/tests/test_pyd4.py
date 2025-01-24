import pytest
import pyd4
import tempfile
import numpy as np
from pathlib import Path

TEST_DATA_DIR = Path(__file__).parent.parent.parent / "d4tools/test"


@pytest.fixture
def temp_d4_file():
    # Create a temporary D4 file
    with tempfile.NamedTemporaryFile(suffix=".d4") as tmp:
        yield tmp.name


def test_view_d4():
    input_file = TEST_DATA_DIR / "data/input.d4"
    truth_file = TEST_DATA_DIR / "show/basic-view/output.txt"

    d4_file = pyd4.D4File(str(input_file))

    truth_intervals = []
    with open(truth_file) as f:
        for line in f:
            chrom, start, end, value = line.strip().split()
            truth_intervals.append((chrom, int(start), int(end), int(value)))

    for truth in truth_intervals:
        chrom, start, end, expected_value = truth
        result = d4_file[f"{chrom}:{start}-{end}"]
        assert all(v == expected_value for v in result), (
            f"Mismatch at interval {chrom}:{start}-{end}"
        )
