import bloop
import bloop.condition
import pytest
import uuid

from bloop.condition import And, Or, Not


def test_duplicate_name_refs(renderer, User):
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


def test_condition_ops(User):
    age, name = (User.age >= 3), (User.name == "foo")

    assert age & name == And(age, name)
    assert age | name == Or(age, name)
    assert ~age == Not(age)


def test_condition_len(User):
    age, name = (User.age >= 3), (User.name == "foo")
    and_condition = age & name
    or_condition = bloop.condition.And(age, name, age)
    not_condition = ~age

    assert len(or_condition) == 3
    assert len(and_condition) == 2
    assert len(age) == len(name) == len(not_condition) == 1


def test_multi_shortcut(User):
    """ And or Or with single conditions render as their sole condition """
    age = User.age >= 3
    condition = bloop.condition.And(age)
    assert condition.conditions == [age]

    condition = bloop.condition.Or(age)
    assert condition.conditions == [age]


def test_multi_chains_flatten(User):
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


def test_not(renderer, User):
    age = ~(User.age >= 3)
    condition = bloop.condition.And(age)
    expected = {"ConditionExpression": "(NOT (#n0 >= :v1))",
                "ExpressionAttributeNames": {"#n0": "age"},
                "ExpressionAttributeValues": {":v1": {"N": "3"}}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_invalid_comparator(User):
    with pytest.raises(ValueError):
        bloop.condition.Comparison(User.age, "not-a-comparator", 5)


def test_attribute_exists(User, renderer):
    exists = User.age.is_not(None)
    expected_exists = {"ConditionExpression": "(attribute_exists(#n0))",
                       "ExpressionAttributeNames": {"#n0": "age"}}

    renderer.render(exists, "condition")
    assert renderer.rendered == expected_exists


def test_attribute_not_exists(User, renderer):
    not_exists = User.age.is_(None)
    expected_not_exists = {
        "ConditionExpression": "(attribute_not_exists(#n0))",
        "ExpressionAttributeNames": {"#n0": "age"}}

    renderer.render(not_exists, "condition")
    assert renderer.rendered == expected_not_exists


def test_begins_with(renderer, User):
    condition = User.name.begins_with("foo")
    expected = {"ConditionExpression": "(begins_with(#n0, :v1))",
                "ExpressionAttributeNames": {"#n0": "name"},
                "ExpressionAttributeValues": {":v1": {"S": "foo"}}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_contains(renderer, User):
    condition = User.name.contains("foo")
    expected = {"ConditionExpression": "(contains(#n0, :v1))",
                "ExpressionAttributeNames": {"#n0": "name"},
                "ExpressionAttributeValues": {":v1": {"S": "foo"}}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_between(renderer, User):
    condition = User.name.between("bar", "foo")
    expected = {"ConditionExpression": "(#n0 BETWEEN :v1 AND :v2)",
                "ExpressionAttributeNames": {"#n0": "name"},
                "ExpressionAttributeValues": {":v1": {"S": "bar"},
                                              ":v2": {"S": "foo"}}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_in(renderer, User):
    condition = User.name.in_(["bar", "foo"])
    expected = {"ConditionExpression": "(#n0 IN (:v1, :v2))",
                "ExpressionAttributeNames": {"#n0": "name"},
                "ExpressionAttributeValues": {":v1": {"S": "bar"},
                                              ":v2": {"S": "foo"}}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_base_condition(renderer, User):
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


def test_render_path(renderer, User):
    """ A path should be rendered as #column.#field.#field """
    path = "foo bar baz".split()
    renderer.name_ref(User.email, path=path)
    expected = {'ExpressionAttributeNames':
                {'#n0': 'email', '#n3': 'baz', '#n2': 'bar', '#n1': 'foo'}}
    assert renderer.rendered == expected


def test_path_comparator(renderer, Document):
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


def test_typedmap_path_comparator(renderer, engine, local_bind):
    """ TypedMap should defer to the value typedef for conditions """
    base = bloop.new_base()

    class Model(base):
        id = bloop.Column(bloop.Integer, hash_key=True)
        data = bloop.Column(bloop.TypedMap(bloop.UUID))
    with local_bind():
        engine.bind(base=base)

    uid = uuid.uuid4()
    condition = Model.data['foo'].is_(uid)

    expected = {
        'ConditionExpression': '(#n0.#n1 = :v2)',
        'ExpressionAttributeValues': {':v2': {'S': str(uid)}},
        'ExpressionAttributeNames': {'#n0': 'data', '#n1': 'foo'}
    }

    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_name_ref_with_path(renderer, engine, local_bind, document_type):
    """ Columns with custom names with literal periods render correctly """
    base = bloop.new_base()

    class Model(base):
        id = bloop.Column(bloop.Integer, hash_key=True, name='this.is.id')
        data = bloop.Column(document_type)
    with local_bind():
        engine.bind(base=base)

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


def test_list_path(renderer, Document):
    """ render list indexes correctly """
    condition = Document.numbers[1] >= 3
    expected = {
        'ExpressionAttributeValues': {':v1': {'N': '3'}},
        'ConditionExpression': '(#n0[1] >= :v1)',
        'ExpressionAttributeNames': {'#n0': 'numbers'}}
    renderer.render(condition, "condition")
    assert renderer.rendered == expected


def test_equality(Document):
    lt = Document.id < 10
    gt = Document.id > 12

    path = Document.data["Rating"] == 3.4

    # Order doesn't matter for multi conditions
    basic_and = lt & gt
    swapped_and = gt & lt
    multiple_and = lt & lt & gt

    basic_or = lt | gt
    swapped_or = gt | lt
    multiple_or = lt | lt | gt

    not_lt = ~lt
    not_gt = ~gt

    not_exists_data = Document.data.is_(None)
    not_exists_id = Document.id.is_(None)
    exists_id = Document.id.is_not(None)

    begins_hello = Document.data["Description"]["Body"].begins_with("hello")
    begins_world = Document.data["Description"]["Body"].begins_with("world")
    begins_numbers = Document.numbers.begins_with(8)

    contains_hello = Document.data["Description"]["Body"].contains("hello")
    contains_world = Document.data["Description"]["Body"].contains("world")
    contains_numbers = Document.numbers.contains(9)

    between_small = Document.id.between(5, 6)
    between_big = Document.id.between(100, 200)
    between_numbers = Document.numbers.between(set([8080]), set([8088]))

    in_small = Document.id.in_([3, 7, 11])
    in_big = Document.id.in_([123, 456])
    in_numbers = Document.numbers.in_([120, 450])

    all_conditions = [
        lt, gt, path,
        basic_and, swapped_and, multiple_and,
        basic_or, swapped_or, multiple_or,
        not_lt, not_gt,
        not_exists_data, not_exists_id, exists_id,
        begins_hello, begins_world, begins_numbers,
        contains_hello, contains_world, contains_numbers,
        between_small, between_big, between_numbers,
        in_small, in_big, in_numbers
    ]

    for first in all_conditions:
        for second in all_conditions:
            if first is second:
                assert first == second
            else:
                assert first != second
