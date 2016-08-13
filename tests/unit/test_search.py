import pytest
from bloop.search import Search, Scan, Query

from ..helpers.models import User

neither = {}
no_index = {"model": User}
no_model = {"index": User.by_email}
both = {"model": User, "index": User.by_email}


@pytest.mark.parametrize("kwargs, cls, expected", [
    (neither, Scan, '<Scan[None]>'),
    (neither, Query, '<Query[None]>'),
    (neither, Search, '<Search[None]>'),

    (no_index, Scan, '<Scan[User]>'),
    (no_index, Query, '<Query[User]>'),
    (no_index, Search, '<Search[User]>'),

    (no_model, Scan, '<Scan[None.by_email]>'),
    (no_model, Query, '<Query[None.by_email]>'),
    (no_model, Search, '<Search[None.by_email]>'),

    (both, Scan, '<Scan[User.by_email]>'),
    (both, Query, '<Query[User.by_email]>'),
    (both, Search, '<Search[User.by_email]>')
])
def test_repr(kwargs, cls, expected):
    obj = cls(**kwargs)
    assert repr(obj) == expected
