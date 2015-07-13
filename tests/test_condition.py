import bloop.condition
import pytest


def test_render(engine, User):
    ''' render modes: condition, filter, key '''

    condition = User.age >= 3
    expected = {'ConditionExpression': '(#n0 >= :v1)',
                'ExpressionAttributeNames': {'#n0': 'age'},
                'ExpressionAttributeValues': {':v1': {'N': '3'}}}
    rendered = bloop.condition.render(engine, condition,
                                      "condition", legacy=False)
    assert expected == rendered


def test_legacy_render(engine, User):
    ''' render modes: condition, filter, key '''

    condition = User.age >= 3
    expected = {'age': {'AttributeValueList': [{'N': '3'}],
                        'ComparisonOperator': 'GE'}}
    rendered = bloop.condition.render(engine, condition,
                                      "condition", legacy=True)
    assert expected == rendered


def test_duplicate_name_refs(engine, User):
    ''' name refs are re-used for the same name '''
    renderer = bloop.condition.ConditionRenderer(engine, legacy=False)
    assert renderer.name_ref(User.age) == renderer.name_ref(User.age) == "#n0"


def test_legacy_refs(engine, User):
    ''' legacy rederers can't do name refs, but value refs are fine '''
    renderer = bloop.condition.ConditionRenderer(engine, legacy=True)
    with pytest.raises(ValueError):
        renderer.name_ref(User.age)
    assert renderer.value_ref(User.age, 5) == {'N': '5'}
