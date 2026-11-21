import pytest


def normalize_sample_id(value):
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned if cleaned else None


def iter_chunks(rows, chunk_size=50_000):
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    for idx in range(0, len(rows), chunk_size):
        yield rows[idx : idx + chunk_size]


def map_phl_status(test_status):
    if test_status is not None and test_status.strip() in {
        "Final",
        "Complete",
        "Submitted",
        "Reported",
    }:
        return "received"
    return "not received"


def test_normalize_sample_id():
    assert normalize_sample_id("  SP-1009  ") == "SP-1009"
    assert normalize_sample_id(40512) == "40512"
    assert normalize_sample_id("   ") is None
    assert normalize_sample_id(None) is None


def test_iter_chunks():
    data = list(range(125_000))
    batches = list(iter_chunks(data, 50_000))
    assert len(batches) == 3
    assert len(batches[0]) == 50_000
    assert len(batches[1]) == 50_000
    assert len(batches[2]) == 25_000


def test_map_phl_status():
    assert map_phl_status("Final") == "received"
    assert map_phl_status("Reported") == "received"
    assert map_phl_status("Pending") == "not received"
    assert map_phl_status(None) == "not received"


def test_iter_chunks_invalid_size():
    with pytest.raises(ValueError):
        list(iter_chunks([1, 2, 3], 0))
