import arrow
import base64
import decimal
import pytest
import uuid
from bloop import types


def symmetric_test(typedef, *pairs):
    """ Test any number of load/dump pairs for a symmetric `Type` instance """
    for (loaded, dumped) in pairs:
        assert typedef.dynamo_load(dumped) == loaded
        assert typedef.dynamo_dump(loaded) == dumped


def test_default_can_load_dump():
    """ Default Type.can_[load|dump] check against [backing|python]_type """

    class MyType(types.Type):
        backing_type = "FOO"
        python_type = float

    typedef = MyType()
    assert typedef.can_load({"FOO": "not_a_float"})
    assert typedef.can_dump(float(10))


def test_load_dump_best_effort():
    """ can_* are not called when trying to load values """

    class MyType(types.Type):
        backing_type = "FOO"
        python_type = float

    typedef = MyType()
    assert typedef._load({"NOT_FOO": "not_a_float"}) == "not_a_float"
    assert typedef._dump("not_a_float") == {"FOO": "not_a_float"}


def test_string():
    typedef = types.String()
    symmetric_test(typedef, ("foo", "foo"))


def test_uuid():
    typedef = types.UUID()
    uuid_obj = uuid.uuid4()
    uuid_str = str(uuid_obj)
    symmetric_test(typedef, (uuid_obj, uuid_str))


def test_datetime():
    typedef = types.DateTime()

    tz = "Europe/Paris"
    now = arrow.now()

    # Not a symmetric type
    assert typedef.dynamo_load(now.isoformat()) == now
    assert typedef.dynamo_dump(now) == now.to("utc").isoformat()

    assert typedef.dynamo_load(now.to(tz).isoformat()) == now
    assert typedef.dynamo_dump(now.to(tz)) == now.to("utc").isoformat()

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
        ("Infinity", TypeError),
        (d("NaN"), TypeError)
    ]
    for value, raises in errors:
        with pytest.raises(raises):
            typedef.dynamo_dump(value)

    symmetric_test(typedef,
                   (1.5, "1.5"),
                   (d(4)/d(3), "1.333333333333333333333333333"))


def test_float_disallow_bool():
    """ Not a strict check in dynamo_dump, but can_dump is overloaded """
    assert not types.Float().can_dump(True)


def test_integer():
    """
    Integer is a thin wrapper over Float that exposes non-decimal objects
    """
    typedef = types.Integer()

    symmetric_test(typedef, (4, "4"))

    assert typedef.dynamo_dump(4.5) == "4"
    assert typedef.dynamo_load("4") == 4

    # Corrupted data is truncated
    assert typedef.dynamo_load("4.5") == 4


def test_binary():
    typedef = types.Binary()
    symmetric_test(typedef, (b"123", "MTIz"), (bytes(1), "AA=="))


def test_sets():

    # Helper since sets are unordered, but dump must return an ordered list
    def check(dumped, expected):
        assert set(dumped) == expected

    tests = [
        (types.Set(types.String),
         set(["Hello", "World"]),
         set(["Hello", "World"])),
        (types.Set(types.Float),
         set([4.5, 3]),
         set(["4.5", "3"])),
        (types.Set(types.Integer),
         set([0, -1, 1]),
         set(["0", "-1", "1"])),
        (types.Set(types.Binary),
         set([b"123", b"456"]),
         set(["MTIz", "NDU2"]))
    ]

    for (typedef, loaded, expected) in tests:
        dumped = typedef.dynamo_dump(loaded)
        check(dumped, expected)
        assert typedef.dynamo_load(expected) == loaded


def test_set_can_dump():
    """ Checks all values in the set """

    typedef = types.Set(types.String)
    assert typedef.can_dump(set(["1", "2", "3"]))
    assert not typedef.can_dump(set(["1", 2, "3"]))


def test_set_type_instance():
    """ Set can take an instance of a Type as well as a Type subclass """
    type_instance = types.String()
    instance_set = types.Set(type_instance)
    assert instance_set.typedef is type_instance

    type_subclass = types.String
    subclass_set = types.Set(type_subclass)
    assert isinstance(subclass_set.typedef, type_subclass)


def test_null():
    typedef = types.Null()

    values = [None, -1, decimal.Decimal(4/3), "string", object()]

    for value in values:
        assert typedef.dynamo_dump(value) is True
        assert typedef.dynamo_load(value) is None


def test_bool():
    """ Boolean will never store/load as empty - bool(None) is False """
    typedef = types.Boolean()

    truthy = [1, True, object(), bool, "str"]
    falsy = [False, None, 0, set(), ""]

    for value in truthy:
        assert typedef.dynamo_dump(value) is True
        assert typedef.dynamo_load(value) is True

    for value in falsy:
        assert typedef.dynamo_dump(value) is False
        assert typedef.dynamo_load(value) is False


def test_map_list_single_value():
    """
    Map and List DO NOT support UUID or DateTime.

    There are no native types for UUID or DateTime, so both use the String
    type in Dynamo - when loading, it's impossible to determine whether the
    loaded value is a String, UUID, or DateTime.

    Strings that represent valid UUIDs or DateTimes will not be
    loaded as such - guessing is a terrible resolution to ambiguity.
    """

    map_typedef = types.Map()
    list_typedef = types.List()
    binary_obj = b"123"
    binary_str = base64.b64encode(binary_obj).decode("utf-8")

    loaded_objs = {
        "string": "value",
        "float": decimal.Decimal("0.125"),
        "int": 4,
        "binary": binary_obj,
        "null": None,
        "boolean": True,
        "map": {"map_str": "map_value", "map_float": decimal.Decimal("0.125")},
        "list": [4, binary_obj]
    }

    dumped_objs = {
        "string": {"S": "value"},
        "float": {"N": "0.125"},
        "int": {"N": "4"},
        "binary": {"B": binary_str},
        "null": {"NULL": None},
        "boolean": {"BOOL": True},
        "map": {"M": {"map_str": {"S": "map_value"},
                      "map_float": {"N": "0.125"}}},
        "list": {"L": [{"N": "4"}, {"B": binary_str}]}
    }

    for key, loaded_obj in loaded_objs.items():
        dumped_obj = dumped_objs[key]

        loaded_map = {"test": loaded_obj}
        dumped_map = {"test": dumped_obj}
        assert map_typedef.dynamo_dump(loaded_map) == dumped_map
        assert map_typedef.dynamo_load(dumped_map) == loaded_map

        loaded_list = [loaded_obj]
        dumped_list = [dumped_obj]
        assert list_typedef.dynamo_dump(loaded_list) == dumped_list
        assert list_typedef.dynamo_load(dumped_list) == loaded_list


def test_map_list_unknown_type():
    """ Trying to load/dump an unknown type raises TypeError """

    class UnknownObject:
        pass

    unknown_obj = UnknownObject()
    unknown_type = {"not S, B, BOOL, etc": unknown_obj}

    with pytest.raises(TypeError):
        types.Map().dynamo_dump({"test": unknown_obj})
    with pytest.raises(TypeError):
        types.List().dynamo_dump([unknown_obj])

    with pytest.raises(TypeError):
        types.Map().dynamo_load({"test": unknown_type})
    with pytest.raises(TypeError):
        types.List().dynamo_load([unknown_type])
