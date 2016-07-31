import uuid

from bloop.tracking import _obj_tracking, get_marked

from .helpers.models import ComplexModel, User


def test_init_marks():
    user = User(id=uuid.uuid4(), unused="unknown kwarg")
    assert _obj_tracking[user]["marked"] == {User.id}


def test_delete_unknown():
    """Even if a field that doesn't exist is deleted, it's marked"""
    user = User(id=uuid.uuid4())
    try:
        del user.email
    except AttributeError:
        # Expected - regardless of the failure to lookup, the remote
        # should expect a delete
        pass
    assert User.email in get_marked(user)


def test_get_update():
    """hash_key shouldn't be in the dumped SET dict"""
    user = User(id=uuid.uuid4(), email="support@domain.com")
    assert User.email in get_marked(user)


def test_tracking_empty_update():
    """no SET changes for hash and range key only"""
    uid = uuid.uuid4()
    model = ComplexModel(name=uid, date="now")
    expected_marked = {ComplexModel.name, ComplexModel.date}
    assert expected_marked == get_marked(model)
