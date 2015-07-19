import bloop.condition
import bloop.tracking
import bloop.util
import uuid


def test_before_save(User, engine):
    ''' before saving, all non-key fields should be in the diff's SET '''
    user = User(id=uuid.uuid4(), age=4)
    expected = {'SET': [(User.age, user.age)]}
    diff = bloop.tracking.diff_obj(user, engine)
    assert diff == expected


def test_update_current(User, engine):
    ''' after update_current, there should be no diff on the object '''
    user = User(id=uuid.uuid4(), age=4)
    bloop.tracking.update_current(user, engine)
    diff = bloop.tracking.diff_obj(user, engine)
    assert not diff


def test_update_del_on_missing(User, engine):
    '''
    for a model with a loaded value, the loaded value should be deleted
    on the next update call where that column is expected but not present
    '''
    # Set age in the object's __tracking__ so we can ensure it's cleared
    user = User(id=uuid.uuid4(), age=4)
    bloop.tracking.update_current(user, engine)
    assert "age" in user.__tracking__

    # Update with no fields, but expect age to be present
    bloop.tracking.update(user, {}, [User.age])
    assert "age" not in user.__tracking__


def test_update_set(User, engine):
    '''
    when an expected value is present, it should be added to the loaded values
    during an update.
    '''
    # Ensure __tracking__ is clear to start
    user = User(id=uuid.uuid4(), age=4)
    bloop.tracking.clear(user)
    assert "age" not in user.__tracking__

    age = engine.__dump__(User, user)["age"]
    expected = {'N': '4'}

    # Update with no fields, but expect age to be present
    bloop.tracking.update(user, {"age": age}, [User.age])
    assert user.__tracking__["age"] == expected


def test_keys_not_in_diff(ComplexModel, engine):
    ''' key columns should never be part of a diff '''
    obj = ComplexModel(name=uuid.uuid4(), date="now")
    bloop.tracking.update_current(obj, engine)
    assert "name" in obj.__tracking__
    assert "date" in obj.__tracking__

    diff = bloop.tracking.diff_obj(obj, engine)
    assert not diff


def test_diff_del(ComplexModel, engine):
    ''' fields removed from the object with del are in the diff's DEL key '''
    obj = ComplexModel(name=uuid.uuid4(), joined="now")
    bloop.tracking.update_current(obj, engine)

    del obj.joined
    expected = {"DELETE": [ComplexModel.joined]}
    diff = bloop.tracking.diff_obj(obj, engine)
    assert diff == expected


def test_atomic_condition(ComplexModel, engine):
    ''' rendered condition uses last loaded values, not current '''
    name = uuid.uuid4()
    obj = ComplexModel(name=name, joined="now")
    bloop.tracking.update_current(obj, engine)

    # Shouldn't see 'then' in expression values
    obj.joined = "then"

    atomic_condition = bloop.tracking.atomic_condition(obj)
    renderer = bloop.condition.ConditionRenderer(engine)
    renderer.render(atomic_condition, 'condition')
    output = renderer.rendered

    # Because obj.Meta.columns is a set, the order that they're rendered in the
    # ConditionExpression is unknown, as well as the name and value refs that
    # they'll have when rendered (#n0 or #n1 or #n2 depending on set iter)

    # Instead of checking literal expression, make sure the expected names and
    # values are present.  Rendering multiple AND is checked in test_condition
    expected_names = ['name', 'email', 'not_projected', 'joined', 'date']
    actual_names = output['ExpressionAttributeNames'].values()
    assert set(actual_names) == set(expected_names)

    ordered = bloop.util.ordered
    expected_values = [{'S': 'now'}, {'S': str(name)}]
    actual_values = list(output['ExpressionAttributeValues'].values())
    assert ordered(actual_values) == ordered(expected_values)
