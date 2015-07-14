import bloop
import pytest


def test_invalid_key(engine, local_bind):
    '''
    filter.key should only accept BeginsWith, Between, and Comparators
    (excluding NE)
    '''
    class Visit(engine.model):
        page = bloop.Column(bloop.String, hash_key=True)
        visitor = bloop.Column(bloop.Integer, range_key=True)
        date = bloop.Column(bloop.DateTime)

        by_date = bloop.GlobalSecondaryIndex(hash_key='date')
    engine.bind()

    q = engine.query(Visit)

    invalid_conditions = [
        # operator.net
        (Visit.page != 'foo'),
        # OR
        ((Visit.page == 'foo') | (Visit.page == 'bar')),
        # NOT
        (~(Visit.page == 'foo')),
        # AttributeNotExists
        (Visit.page.is_(None)),
        # AttributeExists
        (Visit.page.is_not(None)),
        # CONTAINS
        (Visit.page.contains('foo')),
        # IN
        (Visit.page.in_(['foo', 'bar'])),

        # No hash key
        (Visit.date.begins_with('foo')),

        # None
        None,

        # Same column twice in AND
        ((Visit.page.begins_with('f')) & (Visit.page == 'foo')),
        # Too many conditions
        ((Visit.page == 'a') & (Visit.page == 'b') & (Visit.page == 'c')),
        # AND without hash key
        ((Visit.visitor == 0) & (Visit.date == 1))
    ]

    for condition in invalid_conditions:
        with pytest.raises(ValueError):
            q.key(condition)


def test_iterative_key(engine, local_bind):
    ''' iterative .key should AND arguments '''
    class Visit(engine.model):
        page = bloop.Column(bloop.String, hash_key=True)
        visitor = bloop.Column(bloop.Integer, range_key=True)
        date = bloop.Column(bloop.DateTime)

        by_date = bloop.GlobalSecondaryIndex(hash_key='date')
    engine.bind()

    q = engine.query(Visit)

    pcondition = Visit.page == 'foo'
    vcondition = Visit.visitor == 0

    q = q.key(pcondition).key(vcondition)
    assert set(q._key_condition.conditions) == set([vcondition, pcondition])
