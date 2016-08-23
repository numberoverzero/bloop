import pytest

from bloop.tracking import get_marked, get_snapshot, object_deleted, object_loaded, object_saved, sync

from ..helpers.models import User

# Columns are sorted by model name
empty_user_condition = (
    User.age.is_(None) &
    User.email.is_(None) &
    User.id.is_(None) &
    User.joined.is_(None) &
    User.name.is_(None)
)


def test_on_deleted(engine):
    """When an object is deleted, the snapshot expects all columns to be empty"""
    user = User(age=3, name="foo")
    object_deleted.send(engine, obj=user)
    assert get_snapshot(user) == empty_user_condition

    # It doesn't matter if the object had non-empty values saved from a previous sync
    sync(user, engine)
    assert get_snapshot(user) == (
        User.age.is_({"N": "3"}) &
        User.name.is_({"S": "foo"})
    )

    # The deleted signal still clears everything
    object_deleted.send(engine, obj=user)
    assert get_snapshot(user) == empty_user_condition

    # But the current values aren't replaced
    assert user.age == 3
    assert user.name == "foo"


def test_on_loaded_partial(engine):
    """When an object is loaded, the state after loading is snapshotted for future atomic calls"""
    # Creating an instance doesn't snapshot anything
    user = User(age=3, name="foo")
    assert get_snapshot(user) == empty_user_condition

    # Pretend the user was just loaded.  Because only
    # age and name are marked, they will be the only
    # columns included in the snapshot.  A normal load
    # would set the other values to None, and the
    # snapshot would expect those.
    object_loaded.send(engine, obj=user)

    # Values are stored dumped.  Since the dumped flag isn't checked as
    # part of equality testing, we can simply construct the dumped
    # representations to compare.
    assert get_snapshot(user) == (
        User.age.is_({"N": "3"}) &
        User.name.is_({"S": "foo"})
    )


def test_on_loaded_full(engine):
    """Same as the partial test, but with explicit Nones to simulate a real engine.load"""
    user = User(age=3, email=None, id=None, joined=None, name="foo")
    object_loaded.send(engine, obj=user)
    assert get_snapshot(user) == (
        User.age.is_({"N": "3"}) &
        User.email.is_(None) &
        User.id.is_(None) &
        User.joined.is_(None) &
        User.name.is_({"S": "foo"})
    )


def test_on_modified():
    """When an object's values are set or deleted, those columns are marked for tracking"""

    # Creating an instance doesn't mark anything
    user = User()
    assert get_marked(user) == set()

    user.id = "foo"
    assert get_marked(user) == {User.id}

    # Deleting the value does not clear it from the set of marked columns
    del user.id
    assert get_marked(user) == {User.id}

    # Even when the delete fails, the column is marked.
    # We're tracking intention, not state change.
    with pytest.raises(AttributeError):
        del user.age
    assert get_marked(user) == {User.id, User.age}


def test_on_saved(engine):
    """Saving is equivalent to loading w.r.t. tracking.

    The state after saving is snapshotted for future atomic operations."""
    user = User(name="foo", age=3)
    object_saved.send(engine, obj=user)

    # Since "name" and "age" were the only marked columns saved to DynamoDB,
    # they are the only columns that must match for an atomic save.  The
    # state of the other columns wasn't specified, so it's not safe to
    # assume the intended value (missing vs empty)
    assert get_snapshot(user) == (
        User.age.is_({"N": "3"}) &
        User.name.is_({"S": "foo"})
    )
