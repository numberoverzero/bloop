from bloop import (
    UUID,
    BaseModel,
    Binary,
    Column,
    Condition,
    DateTime,
    GlobalSecondaryIndex,
    Integer,
    List,
    LocalSecondaryIndex,
    Map,
    Number,
    Set,
    String,
)


DocumentType = Map(**{
    'Rating': Number(),
    'Stock': Integer(),
    'Description': Map(**{
        'Heading': String,
        'Body': String,
        'Specifications': String
    }),
    'Id': UUID,
    'Updated': DateTime
})


class Document(BaseModel):
    id = Column(Integer, hash_key=True)
    data = Column(DocumentType)
    numbers = Column(List(Integer))
    value = Column(Number)
    another_value = Column(Number)
    some_string = Column(String)
    nested_numbers = Column(List(List(Integer)))


class User(BaseModel):
    id = Column(String, hash_key=True)
    age = Column(Integer)
    name = Column(String)
    email = Column(String)
    joined = Column(DateTime, dynamo_name="j")
    by_email = GlobalSecondaryIndex(hash_key="email", projection="all")


class SimpleModel(BaseModel):
    class Meta:
        table_name = "Simple"
    id = Column(String, hash_key=True)


class ComplexModel(BaseModel):
    class Meta:
        write_units = 2
        read_units = 3
        table_name = "CustomTableName"

    name = Column(UUID, hash_key=True)
    date = Column(String, range_key=True)
    email = Column(String)
    joined = Column(String)
    not_projected = Column(Integer)
    by_email = GlobalSecondaryIndex(hash_key="email", read_units=4, projection="all", write_units=5)
    by_joined = LocalSecondaryIndex(range_key="joined", projection=["email"])


class VectorModel(BaseModel):
    name = Column(String, hash_key=True)
    list_str = Column(List(String))
    set_str = Column(Set(String))
    map_nested = Column(Map(**{
        "bytes": Binary,
        "str": String,
        "map": Map(**{
            "int": Integer,
            "str": String
        })
    }))
    some_int = Column(Integer)
    some_bytes = Column(Binary)


# Provides a gsi and lsi with constrained projections for testing Filter.select validation
class ProjectedIndexes(BaseModel):
    h = Column(Integer, hash_key=True)
    r = Column(Integer, range_key=True)
    both = Column(String)
    neither = Column(String)
    gsi_only = Column(String)
    lsi_only = Column(String)

    by_gsi = GlobalSecondaryIndex(hash_key="h", projection=["both", "gsi_only"])
    by_lsi = LocalSecondaryIndex(range_key="r", projection=["both", "lsi_only"])


conditions = set()


def _build_conditions():
    """This is a method so that we can name each condition before adding it.

    This makes the conditions self-documenting;
    simplifies building compound conditions;
    eases extension for new test cases
    """
    empty = Condition()
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

    begins_hello = Document.some_string.begins_with("hello")
    begins_world = Document.some_string.begins_with("world")

    contains_hello = Document.some_string.contains("hello")
    contains_world = Document.some_string.contains("world")
    contains_numbers = Document.numbers.contains(9)

    between_small = Document.id.between(5, 6)
    between_big = Document.id.between(100, 200)
    between_strings = Document.some_string.between("alpha", "zebra")

    in_small = Document.id.in_([3, 7, 11])
    in_big = Document.id.in_([123, 456])
    in_numbers = Document.numbers.in_([120, 450])

    conditions.update((
        empty,
        lt, gt, path,
        basic_and, swapped_and, multiple_and,
        basic_or, swapped_or, multiple_or,
        not_lt, not_gt,
        not_exists_data, not_exists_id, exists_id,
        begins_hello, begins_world, between_strings,
        contains_hello, contains_world, contains_numbers,
        between_small, between_big, between_strings,
        in_small, in_big, in_numbers
    ))


_build_conditions()
