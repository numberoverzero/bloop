import datetime
import decimal
import uuid

import pytest

from bloop.types import (
    UUID,
    Binary,
    Boolean,
    DateTime,
    Integer,
    List,
    Map,
    Number,
    Set,
    String,
    Timestamp,
    Type,
)

from ..helpers.models import DocumentType


def symmetric_test(typedef, *pairs):
    """ Test any number of load/dump pairs for a symmetric `Type` instance """
    for (loaded, dumped) in pairs:
        assert typedef.dynamo_load(dumped, context={}) == loaded
        assert typedef.dynamo_dump(loaded, context={}) == dumped


def test_missing_abstract_methods():
    """NotImplementedError when dynamo_load or dynamo_dump are missing"""

    class MyType(Type):
        backing_type = "S"
        python_type = str

    typedef = MyType()

    with pytest.raises(NotImplementedError):
        typedef._load({"S": "value"}, context={})

    with pytest.raises(NotImplementedError):
        typedef._dump("value", context={})


def test_load_dump_best_effort(engine):
    """python_type is an informational field, and doesn't check types on load/dump"""

    class MyType(String):
        backing_type = "FOO"
        python_type = float

    typedef = MyType()
    assert "not_a_float" == typedef._load({"NOT_FOO": "not_a_float"}, context={"engine": engine})
    assert {"FOO": "not_a_float"} == typedef._dump("not_a_float", context={"engine": engine})


@pytest.mark.parametrize("typedef", [String, UUID, DateTime, Timestamp, Number, Integer, Binary, Boolean])
def test_none_scalar_types(typedef):
    """single-value types without an explicit 'lack of value' sentinel should return None when given None"""
    type = typedef()
    context = {}

    assert type._load(None, context=context) is None
    assert type._load({typedef.backing_type: None}, context=context) is None
    assert type.dynamo_load(None, context=context) is None

    assert type._dump(None, context=context) is None
    assert type.dynamo_dump(None, context=context) is None


@pytest.mark.parametrize("typedef, default", [
    (Set(String), set()),
    (Set(Integer), set()),
    (Set(Binary), set()),
    (List(DateTime), list()),
    (DocumentType, {
        "Rating": None,
        "Stock": None,
        "Description": {"Heading": None, "Body": None, "Specifications": None},
        "Id": None,
        "Updated": None})
])
def test_load_none_vector_types(engine, typedef, default):
    """multi-value types return empty containers when given None"""
    context = {"engine": engine}

    assert typedef._load(None, context=context) == default
    assert typedef.dynamo_load(None, context=context) == default


@pytest.mark.parametrize("typedef, nones", [
    (Set(String), ([None], [], None)),
    (List(String), ([None], [], None)),
    (DocumentType, ({"Rating": None}, {}, None))
])
def test_dump_none_vector_types(engine, typedef, nones):
    context = {"engine": engine}

    for values in nones:
        assert typedef._dump(values, context=context) is None
        assert typedef.dynamo_dump(values, context=context) is None
        assert typedef.dynamo_dump(None, context=context) is None


@pytest.mark.parametrize("typedef, values, expected", [
    (Set(String), [None, "hello"], ["hello"]),
    (List(String), ["foo", None], [{"S": "foo"}]),
    (DocumentType, {"Rating": 3.0, "Stock": None}, {"Rating": {"N": "3"}})
])
def test_dump_partial_none(engine, typedef, values, expected):
    """vector types filter out inner Nones"""
    assert typedef.dynamo_dump(values, context={"engine": engine}) == expected


def test_string():
    typedef = String()
    symmetric_test(typedef, ("foo", "foo"))


def test_uuid():
    typedef = UUID()
    uuid_obj = uuid.uuid4()
    uuid_str = str(uuid_obj)
    symmetric_test(typedef, (uuid_obj, uuid_str))


def test_datetime():
    typedef = DateTime()
    now = datetime.datetime.now(datetime.timezone.utc)
    now_str = now.isoformat()
    symmetric_test(typedef, (now, now_str))


@pytest.mark.parametrize("naive", (datetime.datetime.now(), datetime.datetime.utcnow()))
def test_datetime_naive(naive):
    """
    Python 3.6 made astimezone assume the system timezone when tzinfo is None.
    Therefore bloop explicitly guards against a naive object.
    """
    typedef = DateTime()
    with pytest.raises(ValueError):
        typedef.dynamo_dump(naive, context=None)


def test_timestamp():
    typedef = Timestamp()
    # .replace because microseconds are dropped
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    # numbers are passed to dynamodb as strings
    now_str = str(int(now.timestamp()))
    symmetric_test(typedef, (now, now_str))


@pytest.mark.parametrize("naive", (datetime.datetime.now(), datetime.datetime.utcnow()))
def test_timestamp_naive(naive):
    """
    Python 3.6 made astimezone assume the system timezone when tzinfo is None.
    Therefore bloop explicitly guards against a naive object.
    """
    typedef = Timestamp()
    with pytest.raises(ValueError):
        typedef.dynamo_dump(naive, context=None)


def test_number():
    typedef = Number()
    d = decimal.Decimal
    symmetric_test(typedef, (1.5, "1.5"), (d(4) / d(3), "1.333333333333333333333333333"))


@pytest.mark.parametrize(
    "value, raises",
    [
        (decimal.Decimal(4 / 3), decimal.Inexact),
        (decimal.Decimal(10) ** 900, decimal.Overflow),
        (decimal.Decimal(0.9) ** 9000, decimal.Underflow),
        ("Infinity", TypeError),
        (decimal.Decimal("NaN"), TypeError)])
def test_float_errors(value, raises):
    with pytest.raises(raises):
        Number().dynamo_dump(value, context={})


def test_integer():
    """Integer is a thin wrapper over Float that exposes non-decimal objects"""
    typedef = Integer()
    symmetric_test(typedef, (4, "4"))

    assert typedef.dynamo_dump(4.5, context={}) == "4"
    assert typedef.dynamo_load("4", context={}) == 4

    # Corrupted data is truncated
    assert typedef.dynamo_load("4.5", context={}) == 4


def test_binary():
    typedef = Binary()
    symmetric_test(typedef, (b"123", "MTIz"), (bytes(1), "AA=="))


@pytest.mark.parametrize(
    "set_type, loaded, dumped", [
        (String, {"Hello", "World"}, ["Hello", "World"]),
        (Number, {4.5, 3}, ["4.5", "3"]),
        (Integer, {0, -1, 1}, ["0", "-1", "1"]),
        (Binary, {b"123", b"456"}, ["MTIz", "NDU2"])], ids=str)
def test_sets(engine, set_type, loaded, dumped):
    typedef = Set(set_type)

    assert typedef.dynamo_load(dumped, context={"engine": engine}) == loaded

    # Can't use a simple set because the values are unhashable dicts like {"S": "value"}
    actual_dumped = typedef.dynamo_dump(loaded, context={"engine": engine})
    assert len(actual_dumped) == len(dumped)
    for item in actual_dumped:
        assert item in dumped


def test_set_type_instance():
    """Set can take an instance of a Type as well as a Type subclass"""
    type_instance = String()
    instance_set = Set(type_instance)
    assert instance_set.inner_typedef is type_instance

    type_subclass = String
    subclass_set = Set(type_subclass)
    assert isinstance(subclass_set.inner_typedef, type_subclass)


def test_set_illegal_backing_type():
    """The backing type for a set MUST be one of S/N/B, not BOOL"""
    for typedef in [Boolean, Set(Integer)]:
        with pytest.raises(TypeError):
            Set(typedef)


@pytest.mark.parametrize("value", [1, True, object(), bool, "str", False, 0, set(), ""], ids=repr)
def test_bool(value):
    """Boolean handles all values except None with bool(value)"""
    typedef = Boolean()
    assert typedef.dynamo_dump(value, context={}) is bool(value)
    assert typedef.dynamo_load(value, context={}) is bool(value)


def test_list(engine):
    typedef = List(UUID)
    loaded = [uuid.uuid4() for _ in range(5)]
    expected = [{"S": str(id)} for id in loaded]

    dumped = typedef.dynamo_dump(loaded, context={"engine": engine})
    assert dumped == expected
    assert typedef.dynamo_load(dumped, context={"engine": engine}) == loaded


@pytest.mark.parametrize("key", [object(), None, 3, True])
def test_list_path(key):
    """To support paths in condition expressions, __getitem__ must return another Type.  List only has one Type."""
    inner = UUID()
    typedef = List(inner)
    assert typedef[key] is inner


def test_map_dump(engine):
    """Map handles nested maps and custom types"""
    now = datetime.datetime.now(datetime.timezone.utc)
    loaded = {
        'Rating': 0.5,
        'Stock': 3,
        'Description': {
            'Heading': "Head text",
            'Body': "Body text",
            # Explicit None
            'Specifications': None
        },
        # Id missing entirely
        'Updated': now
    }
    expected = {
        'Rating': {'N': '0.5'},
        'Stock': {'N': '3'},
        'Description': {
            'M': {
                'Heading': {'S': 'Head text'},
                'Body': {'S': 'Body text'}}},
        'Updated': {'S': now.isoformat()}
    }
    dumped = DocumentType.dynamo_dump(loaded, context={"engine": engine})
    assert dumped == expected


def test_map_load(engine):
    """Map handles nested maps and custom types"""
    uid = uuid.uuid4()
    dumped = {
        'Rating': {'N': '0.5'},
        'Stock': {'N': '3'},
        'Description': {
            'M': {
                'Heading': {'S': 'Head text'},
                'Body': {'S': 'Body text'}}},
        'Id': {'S': str(uid)}
    }
    expected = {
        'Rating': 0.5,
        'Stock': 3,
        'Description': {
            'Heading': "Head text",
            'Body': "Body text",
            'Specifications': None
        },
        'Id': uid,
        'Updated': None
    }
    loaded = DocumentType.dynamo_load(dumped, context={"engine": engine})
    assert loaded == expected


def test_map_path():
    """To support paths in condition expressions, __getitem__ must return another Type.

    Unlike List, Map can return a different Type for each of its keys."""
    foo_type = UUID()
    bar_type = Integer()
    bool_type = Boolean()
    typedef = Map(**{
        "foo": foo_type,
        "bar": bar_type,
        "bool": bool_type,
    })
    assert typedef["foo"] is foo_type
    assert typedef["bar"] is bar_type
    assert typedef["bool"] is bool_type


def test_repr():
    typedef = Type()
    assert repr(typedef) == "<Type[None:None]>"

    # Not all python types will be classes
    typedef.backing_type = "foo"
    typedef.python_type = 3
    assert repr(typedef) == "<Type[foo:3]>"

    set_typedef = Set(Integer)
    assert repr(set_typedef) == "<Set[NS:Set]>"
