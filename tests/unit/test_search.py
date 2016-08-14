import collections
import pytest
from bloop.search import ScanIterator, QueryIterator, SearchIterator, search_repr
from bloop.util import Sentinel

from ..helpers.models import User


def simple_iter(session, cls=SearchIterator):
    return cls(
        session=session,
        model=User,
        index=None,
        limit=None,
        request={},
        projected=set()
    )


def test_search_repr():
    cls = type("Class", tuple(), {})
    model = type("Model", tuple(), {})
    index = type("Index", tuple(), {"model_name": "by_gsi"})()

    for has_model, has_index, expected in [
        (None, None, "<Class[None]>"),
        (None, True, "<Class[None.by_gsi]>"),
        (True, None, "<Class[Model]>"),
        (True, True,  "<Class[Model.by_gsi]>"),
    ]:
        assert search_repr(cls, has_model and model, has_index and index) == expected


def test_iterator_reset(session):
    """reset clears buffer, count, scanned, exhausted, yielded"""
    iterator = simple_iter(session)

    # Pretend we've stepped the iterator a few times
    iterator.yielded = 8
    iterator.count = 9
    iterator.scanned = 12
    iterator.buffer.append("obj")
    iterator._exhausted = True

    iterator.reset()

    # Ready to go again, buffer empty and counters reset
    assert iterator.yielded == 0
    assert iterator.count == 0
    assert iterator.scanned == 0
    assert len(iterator.buffer) == 0
    assert not iterator._exhausted


@pytest.mark.parametrize("limit", [None, 1])
@pytest.mark.parametrize("yielded", [0, 1, 2])
@pytest.mark.parametrize("buffer_size", [0, 1])
@pytest.mark.parametrize("has_tokens", [False, True])
def test_iterator_exhausted(session, limit, yielded, buffer_size, has_tokens):
    """Various states for the buffer's limit, yielded, _exhausted, and buffer.

    Exhausted if either:
    1. The iterator has a limit, and it's yielded at least that many items.
    2. The iterator has run out of continuation tokens, and the buffer is empty.

    Any other combination of states is not exhausted.
    """
    iterator = simple_iter(session)

    iterator.limit = limit
    iterator.yielded = yielded
    iterator.buffer = collections.deque([True] * buffer_size)
    iterator._exhausted = not has_tokens

    should_be_exhausted = (limit and yielded >= limit) or (not buffer_size and not has_tokens)
    assert iterator.exhausted == should_be_exhausted


def test_iterator_next_limit_reached(session):
    """If the iterator has yielded >= limit, next raises (regardless of buffer, continue tokens)"""
    iterator = simple_iter(session)

    # Put something in the buffer so that isn't the cause of StopIteration
    iterator.buffer.append(True)

    iterator.limit = 1
    iterator.yielded = 1
    assert next(iterator, None) is None

    iterator.limit = 1
    iterator.yielded = 2
    assert next(iterator, None) is None


@pytest.mark.parametrize("iterator_cls", [QueryIterator, ScanIterator])
def test_next_states(session, iterator_cls):
    """This monster tests all of the effective cycles for the __next__ machinery.

    As such, it can be daunting.  Here's a quick rundown, ordered by appearance:

    Sentinels:
        Placeholders for values.  This ensures the iterator doesn't depend on the structure of the
        items and continuation token.  If it did, these would almost certainly break it.
    response:
        This fills in the required response structure from a single query/scan call:
            "Count", "ScannedCount" are required fields but not important.
            "LastEvaluatedKey" tells the iterator if there are more pages available.  When it's not falsey,
                               it should be fed directly back into the next request's "ExclusiveStartKey".
            "Item" is the number of items in this page.  By passing 0, 1, or 2 we can verify that the buffer is
                   fully drained before the next page is loaded.  It also lets us verify that the while loop will
                   follow LastEvaluatedKeys until it hits a non-empty page.
    build_responses:
        This expands a compact integer description of a set of pages into the appropriate response structure.
        For example: [0, 2, 1] expands into (0 results, proceed) -> (2 results, proceed) -> (1 result, stop).
        We'll also use those integers in the verifier to compare number of results and number of calls.
    calls_for_current_steps:
        The number of calls that are required to iterate the given chain in the given number of steps.
        For example, the chain [3, 0, 1, 4] has the following table:
            ===== =====
            steps calls
            ===== =====
              0     1
              1     1
              2     1
              3     1
              4     3
              5     4
              6     4
              7     4
              8     4
              9     4
            ===== =====
    verify_iterator:
        Build a new iterator for the given chain, load the responses into the mock session, and then
        carefully advance the iterator until it raises StopIteration.  At each step, check that the correct number
        of calls have been made to session.search_items.
    """

    data = Sentinel("data")
    proceed = Sentinel("proceed")

    def response(count, terminate=False):
        return {
            "Count": count,
            "ScannedCount": count * 3,
            "Items": [data] * count,
            "LastEvaluatedKey": None if terminate else proceed
        }

    def build_responses(chain):
        return [response(count) for count in chain[:-1]] + [response(chain[-1], True)]

    def calls_for_current_steps(chain, current_steps):
        required_steps = 0
        call_count = 0
        for call_count, page in enumerate(chain):
            required_steps += page
            if required_steps >= current_steps:
                break
        return call_count + 1

    def verify_iterator(chain):
        iterator = simple_iter(session, cls=iterator_cls)
        # VERY IMPORTANT!  Without the reset, calls from the previous chain
        # will count against this chain.
        session.search_items.reset_mock()
        session.search_items.side_effect = build_responses(chain)

        current_steps = 0
        result = data
        while result is data:
            current_steps += 1
            result = next(iterator, None)
            expected_call_count = calls_for_current_steps(chain, current_steps)
            assert session.search_items.call_count == expected_call_count
        assert iterator.yielded == current_steps - 1

    # Here are the possible boundaries for pagination:
    # 0 Results
    #   [   ]  # 1 call
    #   [ | ]  # 2 call
    # 1 Results
    #   [   ✔   ] # 1 call
    #   [   | ✔ ] # 2 calls
    #   [ ✔ |   ] # 2 calls
    # N Results
    #   [  ✔₁   ✔₂  ] # 1 call
    #   [   | ✔₁ ✔₂ ] # 2 calls
    #   [ ✔₁  |  ✔₂ ] # 2 calls
    #   [ ✔₁ ✔₂ |   ] # 2 calls
    # Additional calls are repetitions of the above
    chains = [
        [0], [0, 0],
        [1], [0, 1], [1, 0],
        [2], [0, 2], [1, 1], [2, 0]
    ]
    for chain in chains:
        verify_iterator(chain)
