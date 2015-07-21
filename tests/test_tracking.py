import bloop.condition
import bloop.tracking
import bloop.util
import uuid


def test_before_save(User, engine):
    """ before saving, all non-key fields should be in the diff's SET """
    user = User(id=uuid.uuid4(), age=4)
    expected = {"SET": [(User.age, user.age)]}
    diff = bloop.tracking.diff_obj(user, engine)
    assert diff == expected


def test_update_current(User, engine):
    """ after update_current, there should be no diff on the object """
    user = User(id=uuid.uuid4(), age=4)
    bloop.tracking.update_current(user, engine)
    diff = bloop.tracking.diff_obj(user, engine)
    assert not diff


def test_update_del_on_missing(User, engine):
    """
    for a model with a loaded value, the loaded value should be deleted
    on the next update call where that column is expected but not present
    """
    # Set age in the object's __tracking so we can ensure it's cleared
    user = User(id=uuid.uuid4(), age=4)
    bloop.tracking.update_current(user, engine)
    assert "age" in user.__tracking["values"]

    # Update with no fields, but expect age to be present
    bloop.tracking.update(user, {}, [User.age])
    assert "age" not in user.__tracking["values"]


def test_update_set(User, engine):
    """
    when an expected value is present, it should be added to the loaded values
    during an update.
    """
    # Ensure __tracking is clear to start
    user = User(id=uuid.uuid4(), age=4)
    bloop.tracking.clear(user)
    assert "age" not in user.__tracking["values"]

    age = engine._dump(User, user)["age"]
    expected = {"N": "4"}

    # Update with no fields, but expect age to be present
    bloop.tracking.update(user, {"age": age}, [User.age])
    assert user.__tracking["values"]["age"] == expected


def test_keys_not_in_diff(ComplexModel, engine):
    """ key columns should never be part of a diff """
    obj = ComplexModel(name=uuid.uuid4(), date="now")
    bloop.tracking.update_current(obj, engine)
    assert "name" in obj.__tracking["values"]
    assert "date" in obj.__tracking["values"]

    diff = bloop.tracking.diff_obj(obj, engine)
    assert not diff


def test_diff_del(ComplexModel, engine):
    """ fields removed from the object with del are in the diff's DEL key """
    obj = ComplexModel(name=uuid.uuid4(), joined="now")
    bloop.tracking.update_current(obj, engine)

    del obj.joined
    expected = {"DELETE": [ComplexModel.joined]}
    diff = bloop.tracking.diff_obj(obj, engine)
    assert diff == expected


def test_atomic_condition(ComplexModel, engine):
    """ rendered condition uses last loaded values, not current """
    name = uuid.uuid4()
    obj = ComplexModel(name=name, joined="now")
    # Expect all columns (except name, joined) to not exist
    bloop.tracking.update_current(obj, engine)

    # Shouldn't see "then" in expression values
    obj.joined = "then"

    atomic_condition = bloop.tracking.atomic_condition(obj)
    renderer = bloop.condition.ConditionRenderer(engine)
    renderer.render(atomic_condition, "condition")
    condition = (
        "(((((attribute_not_exists(#n0)) AND (attribute_not_exists(#n1))) AND"
        " (#n2 = :v3)) AND (#n4 = :v5)) AND (attribute_not_exists(#n6)))")
    expected = {
        "ExpressionAttributeNames": {"#n2": "joined", "#n4": "name",
                                     "#n0": "date", "#n1": "email",
                                     "#n6": "not_projected"},
        "ExpressionAttributeValues": {":v3": {"S": "now"},
                                      ":v5": {"S": str(name)}},
        "ConditionExpression": condition}

    assert renderer.rendered == expected


def test_atomic_partial_load(ComplexModel, engine):
    """
    When only some columns are loaded, an atomic condition should only build
    conditions on those columns - not on all columns of the model.
    """
    obj = ComplexModel(name=uuid.uuid4(), joined="now")
    attrs = engine._dump(ComplexModel, obj)
    expected = [ComplexModel.name, ComplexModel.joined]
    bloop.tracking.update(obj, attrs, expected)

    atomic_condition = bloop.tracking.atomic_condition(obj)
    renderer = bloop.condition.ConditionRenderer(engine)
    renderer.render(atomic_condition, "condition")
    expected = {
        'ExpressionAttributeValues': {
            ':v3': {'S': str(obj.name)}, ':v1': {'S': 'now'}},
        'ExpressionAttributeNames': {'#n0': 'joined', '#n2': 'name'},
        'ConditionExpression': '((#n0 = :v1) AND (#n2 = :v3))'}
    assert renderer.rendered == expected
