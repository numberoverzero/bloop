import arrow
import decimal
import pytest
import uuid
from bloop import types


def symmetric_test(typedef, *pairs):
    ''' Test any number of load/dump pairs for a symmetric `Type` instance '''
    for (loaded, dumped) in pairs:
        assert typedef.dynamo_load(dumped) == loaded
        assert typedef.dynamo_dump(loaded) == dumped


def test_default_can_load_dump():
    ''' Default Type.can_[load|dump] check against [backing|python]_type '''

    class MyType(types.Type):
        backing_type = 'FOO'
        python_type = float

    typedef = MyType()
    assert typedef.can_load({'FOO': 'not_a_float'})
    assert typedef.can_dump(float(10))


def test_load_dump_best_effort():
    ''' can_* are not called when trying to load values '''

    class MyType(types.Type):
        backing_type = 'FOO'
        python_type = float

    typedef = MyType()
    assert typedef.__load__({'NOT_FOO': 'not_a_float'}) == 'not_a_float'
    assert typedef.__dump__('not_a_float') == {'FOO': 'not_a_float'}


def test_string():
    typedef = types.String()
    symmetric_test(typedef, (None, None), ("foo", "foo"))


def test_uuid():
    typedef = types.UUID()
    uuid_obj = uuid.uuid4()
    uuid_str = str(uuid_obj)
    symmetric_test(typedef, (None, None), (uuid_obj, uuid_str))


def test_datetime():
    typedef = types.DateTime()

    tz = 'Europe/Paris'
    now = arrow.now()

    # Not a symmetric type
    assert typedef.dynamo_load(None) is None
    assert typedef.dynamo_dump(None) is None

    assert typedef.dynamo_load(now.isoformat()) == now
    assert typedef.dynamo_dump(now) == now.to('utc').isoformat()

    assert typedef.dynamo_load(now.to(tz).isoformat()) == now
    assert typedef.dynamo_dump(now.to(tz)) == now.to('utc').isoformat()

    # Should load values in the given timezone.
    # Because arrow objects compare equal regardless of timezone, we
    # isoformat each to compare the rendered strings (which preserve tz).
    local_typedef = types.DateTime(timezone=tz)
    loaded_as_string = local_typedef.dynamo_load(now.isoformat()).isoformat()
    now_with_tz_as_string = now.to(tz).isoformat()
    assert loaded_as_string == now_with_tz_as_string


def test_float():
    typedef = types.Float()
    d = decimal.Decimal

    errors = [
        (d(4/3), decimal.Inexact),
        (d(10) ** 900, decimal.Overflow),
        (d(0.9) ** 9000, decimal.Underflow),
        ('Infinity', TypeError),
        (d('NaN'), TypeError)
    ]
    for value, raises in errors:
        with pytest.raises(raises):
            typedef.dynamo_dump(value)

    symmetric_test(typedef,
                   (None, None),
                   (1.5, '1.5'),
                   (d(4)/d(3), "1.333333333333333333333333333"))


def test_float_disallow_bool():
    ''' Not a strict check in dynamo_dump, but can_dump is overloaded '''
    assert not types.Float().can_dump(True)


def test_integer():
    '''
    Integer is a thin wrapper over Float that exposes non-decimal objects
    '''
    typedef = types.Integer()

    symmetric_test(typedef, (None, None), (4, '4'))

    assert typedef.dynamo_dump(4.5) == '4'
    assert typedef.dynamo_load('4') == 4

    # Corrupted data is truncated
    assert typedef.dynamo_load('4.5') == 4


def test_binary():
    typedef = types.Binary()
    symmetric_test(typedef, (None, None), (b'123', 'MTIz'), (bytes(1), 'AA=='))


def test_sets():

    # Helper since sets are unordered, but dump must return an ordered list
    def check(dumped, expected):
        assert set(dumped) == expected

    tests = [
        (types.StringSet(),
         set(["Hello", "World"]),
         set(["Hello", "World"])),
        (types.FloatSet(),
         set([4.5, 3, None]),
         set(["4.5", "3", None])),
        (types.IntegerSet(),
         set([None, 0, -1, 1]),
         set([None, "0", "-1", "1"])),
        (types.BinarySet(),
         set([b'123', b'456']),
         set(['MTIz', 'NDU2']))
    ]

    for (typedef, loaded, expected) in tests:
        dumped = typedef.dynamo_dump(loaded)
        check(dumped, expected)
        assert typedef.dynamo_load(expected) == loaded

    # Any set type will do
    typedef = types.IntegerSet()
    assert typedef.dynamo_dump(None) is None
    assert typedef.dynamo_load(None) is None


def test_set_can_dump():
    ''' Checks all values in the set '''

    typedef = types.StringSet()
    assert typedef.can_dump(set(["1", "2", "3"]))
    assert not typedef.can_dump(set(["1", 2, "3"]))


def test_null():
    typedef = types.Null()

    values = [None, -1, decimal.Decimal(4/3), "string", object()]

    for value in values:
        assert typedef.dynamo_dump(value) is True
        assert typedef.dynamo_load(value) is None


def test_bool():
    ''' Boolean will never store/load as empty - bool(None) is False '''
    typedef = types.Boolean()

    truthy = [1, True, object(), bool, "str"]
    falsy = [False, None, 0, set(), ""]

    for value in truthy:
        assert typedef.dynamo_dump(value) is True
        assert typedef.dynamo_load(value) is True

    for value in falsy:
        assert typedef.dynamo_dump(value) is False
        assert typedef.dynamo_load(value) is False
