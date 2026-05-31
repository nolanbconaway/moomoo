import pytest

from moomoo_navidrome.utils_ import batched


def test_batched_even_division():
    assert list(batched([1, 2, 3, 4], 2)) == [(1, 2), (3, 4)]


def test_batched_last_batch_shorter():
    assert list(batched("ABCDEFG", 3)) == [("A", "B", "C"), ("D", "E", "F"), ("G",)]


def test_batched_empty_iterable_returns_no_batches():
    assert list(batched([], 3)) == []


def test_batched_accepts_iterators():
    assert list(batched(iter(range(5)), 2)) == [(0, 1), (2, 3), (4,)]


@pytest.mark.parametrize("n", [0, -1])
def test_batched_raises_when_n_less_than_one(n):
    with pytest.raises(ValueError, match="n must be at least one"):
        list(batched([1, 2, 3], n))
