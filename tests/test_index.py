import bloop.index
import pytest


def test_projection_validation():
    ''' should be all, keys_only, or a list of column model names '''
    Index = bloop.index.Index

    with pytest.raises(ValueError):
        Index(projection='foo')
    with pytest.raises(ValueError):
        Index(projection=object())
    with pytest.raises(ValueError):
        Index(projection=['only strings', 1, None])

    assert Index(projection='all').projection == 'ALL'
    assert Index(projection='keys_only').projection == 'KEYS_ONLY'
    assert Index(projection=['foo', 'bar']).projection == ['foo', 'bar']
