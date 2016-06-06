import bloop
import bloop.condition
import pytest
import uuid

from bloop.condition import And, Or, Not

from test_models import User, Document, DocumentType, conditions


@pytest.fixture
def renderer(engine):
    return bloop.condition.ConditionRenderer(engine)


def test_duplicate_name_refs(renderer):
    """ name refs are re-used for the same name """
    assert renderer.name_ref(User.age) == renderer.name_ref(User.age) == "#n0"


def test_no_refs(renderer):
    """
    when name/value refs are missing, ExpressionAttributeNames/Values
    aren't populated """
    condition = And()
    expected = {"ConditionExpression": "()"}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_condition_ops():
    age, name = (User.age >= 3), (User.name == "foo")

    assert age & name == And(age, name)
    assert age | name == Or(age, name)
    assert ~age == Not(age)


def test_condition_len():
    age, name = (User.age >= 3), (User.name == "foo")
    and_condition = age & name
    or_condition = bloop.condition.And(age, name, age)
    not_condition = ~age

    assert len(or_condition) == 3
    assert len(and_condition) == 2
    assert len(age) == len(name) == len(not_condition) == 1


def test_multi_shortcut():
    """ And or Or with single conditions render as their sole condition """
    age = User.age >= 3
    condition = bloop.condition.And(age)
    assert condition.conditions == [age]

    condition = bloop.condition.Or(age)
    assert condition.conditions == [age]


def test_multi_chains_flatten():
    """
    ((condition & condition) & condition) flattens the AND into one condition
    """
    age = User.age >= 3
    name = User.name == "foo"
    email = User.email != "bar"

    and_condition = bloop.condition.Condition()
    or_condition = bloop.condition.Condition()
    for c in [age, name, email]:
        and_condition &= c
        or_condition |= c
    assert and_condition == bloop.condition.And(age, name, email)
    assert or_condition == bloop.condition.Or(age, name, email)


def test_not(renderer):
    age = ~(User.age >= 3)
    condition = bloop.condition.And(age)
    expected = {"ConditionExpression": "(NOT (#n0 >= :v1))",
                "ExpressionAttributeNames": {"#n0": "age"},
                "ExpressionAttributeValues": {":v1": {"N": "3"}}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_invalid_comparator():
    with pytest.raises(ValueError):
        bloop.condition.Comparison(User.age, "not-a-comparator", 5)


def test_attribute_exists(renderer):
    exists = User.age.is_not(None)
    expected_exists = {"ConditionExpression": "(attribute_exists(#n0))",
                       "ExpressionAttributeNames": {"#n0": "age"}}

    renderer.render(exists, "condition")
    assert renderer.rendered == expected_exists


def test_attribute_not_exists(renderer):
    not_exists = User.age.is_(None)
    expected_not_exists = {
        "ConditionExpression": "(attribute_not_exists(#n0))",
        "ExpressionAttributeNames": {"#n0": "age"}}

    renderer.render(not_exists, "condition")
    assert renderer.rendered == expected_not_exists


def test_begins_with(renderer):
    condition = User.name.begins_with("foo")
    expected = {"ConditionExpression": "(begins_with(#n0, :v1))",
                "ExpressionAttributeNames": {"#n0": "name"},
                "ExpressionAttributeValues": {":v1": {"S": "foo"}}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_contains(renderer):
    condition = User.name.contains("foo")
    expected = {"ConditionExpression": "(contains(#n0, :v1))",
                "ExpressionAttributeNames": {"#n0": "name"},
                "ExpressionAttributeValues": {":v1": {"S": "foo"}}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_between(renderer):
    condition = User.name.between("bar", "foo")
    expected = {"ConditionExpression": "(#n0 BETWEEN :v1 AND :v2)",
                "ExpressionAttributeNames": {"#n0": "name"},
                "ExpressionAttributeValues": {":v1": {"S": "bar"},
                                              ":v2": {"S": "foo"}}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_in(renderer):
    condition = User.name.in_(["bar", "foo"])
    expected = {"ConditionExpression": "(#n0 IN (:v1, :v2))",
                "ExpressionAttributeNames": {"#n0": "name"},
                "ExpressionAttributeValues": {":v1": {"S": "bar"},
                                              ":v2": {"S": "foo"}}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_base_condition(renderer):
    """ (Condition() OP condition) is condition """
    base = bloop.condition.Condition()
    other = User.email == "foo"

    assert (base & other) is other
    assert (base | other) is other
    assert (~base) is base
    assert len(base) == 0

    assert base.render(None) is None

    renderer.render(base, "condition")
    assert not renderer.rendered


def test_render_path(renderer):
    """ A path should be rendered as #column.#field.#field """
    path = "foo bar baz".split()
    renderer.name_ref(User.email, path=path)
    expected = {'ExpressionAttributeNames':
                {'#n0': 'email', '#n3': 'baz', '#n2': 'bar', '#n1': 'foo'}}
    assert renderer.rendered == expected


def test_path_comparator(renderer):
    """ Render paths for operations, comparisons, and multi-conditions """

    rating = Document.data["Rating"] > 0.5
    no_body = Document.data["Description"]["Body"].is_(None)
    stock = Document.data["Stock"].in_([1, 2, 3])
    condition = (rating & no_body) | stock

    expected = {
        'ConditionExpression': (
            '(((#n0.#n1 > :v2) AND (attribute_not_exists(#n0.#n3.#n4))) '
            'OR (#n0.#n5 IN (:v6, :v7, :v8)))'),
        'ExpressionAttributeValues': {
            ':v8': {'N': '3'}, ':v7': {'N': '2'},
            ':v6': {'N': '1'}, ':v2': {'N': '0.5'}},
        'ExpressionAttributeNames': {
            '#n0': 'data', '#n3': 'Description',
            '#n5': 'Stock', '#n1': 'Rating', '#n4': 'Body'}}

    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_typedmap_path_comparator(renderer, engine):
    """ TypedMap should defer to the value typedef for conditions """
    class Model(bloop.new_base()):
        id = bloop.Column(bloop.Integer, hash_key=True)
        data = bloop.Column(bloop.TypedMap(bloop.UUID))
    engine.bind(base=Model)

    uid = uuid.uuid4()
    condition = Model.data['foo'].is_(uid)

    expected = {
        'ConditionExpression': '(#n0.#n1 = :v2)',
        'ExpressionAttributeValues': {':v2': {'S': str(uid)}},
        'ExpressionAttributeNames': {'#n0': 'data', '#n1': 'foo'}
    }

    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_name_ref_with_path(renderer, engine):
    """ Columns with custom names with literal periods render correctly """
    class Model(bloop.new_base()):
        id = bloop.Column(bloop.Integer, hash_key=True, name='this.is.id')
        data = bloop.Column(DocumentType)
    engine.bind(base=Model)

    no_id = Model.id.is_(None)
    path_condition = Model.data["Rating"] >= 2
    condition = no_id & path_condition

    expected = {
        'ExpressionAttributeNames': {
            '#n0': 'this.is.id', '#n2': 'Rating', '#n1': 'data'},
        'ExpressionAttributeValues': {':v3': {'N': '2'}},
        'ConditionExpression':
            '((attribute_not_exists(#n0)) AND (#n1.#n2 >= :v3))'}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_list_path(renderer):
    """ render list indexes correctly """
    condition = Document.numbers[1] >= 3
    expected = {
        'ExpressionAttributeValues': {':v1': {'N': '3'}},
        'ConditionExpression': '(#n0[1] >= :v1)',
        'ExpressionAttributeNames': {'#n0': 'numbers'}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


# parametrizing conditions x conditions makes the test count explode into a
# useless number, so we only parametrize one. This should still make isolating
# failures easier, from O(len(conditions*conditions)) when neither
# is parametrized to O(len(conditions))
@pytest.mark.parametrize("condition", conditions, ids=str)
def test_equality(condition):
    for other in conditions:
        if condition is other:
            assert condition == other
        else:
            assert condition != other
