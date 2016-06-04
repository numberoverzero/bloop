import uuid
import bloop.tracking


def test_init_marks(User):
    user = User(id=uuid.uuid4(), unused="unknown kwarg")
    assert bloop.tracking._obj_tracking[user]["marked"] == set([User.id])


def test_delete_unknown(User):
    """ Even if a field that doesn't exist is deleted, it's marked """
    user = User(id=uuid.uuid4())
    try:
        del user.email
    except AttributeError:
        # Expected - regardless of the failure to lookup, the remote
        # should expect a delete
        pass
    assert User.email in bloop.tracking._obj_tracking[user]["marked"]

    diff = bloop.tracking.get_update(user)
    assert diff["REMOVE"] == [User.email]


def test_get_update(User):
    """ hash_key shouldn't be in the dumped SET dict """
    user = User(id=uuid.uuid4(), email="support@domain.com")
    diff = bloop.tracking.get_update(user)

    assert "REMOVE" not in diff
    assert diff["SET"] == [(User.email, "support@domain.com")]


def test_tracking_empty_update(ComplexModel, engine):
    """ no SET changes for hash and range key only """
    uid = uuid.uuid4()
    model = ComplexModel(name=uid, date="now")
    expected_marked = set([ComplexModel.name, ComplexModel.date])
    assert expected_marked == bloop.tracking._obj_tracking[model]["marked"]

    # Dynamo doesn't send the Key (hash/range) as part of the UPDATE expr
    update = bloop.tracking.get_update(model)
    assert not update
