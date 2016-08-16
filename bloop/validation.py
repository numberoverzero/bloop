from .condition import And, BeginsWith, Between, Comparison
from .exceptions import InvalidKeyCondition


def validate_key_condition(model, index, key):
    # Model will always be provided, but Index has priority
    query_on = index or model.Meta

    # `Model_or_Index.hash_key == value`
    # Valid for both (hash,) and (hash, range)
    if check_hash_key(query_on, key):
        return

    # Failed.  Without a range key, the check above is the only valid key condition.
    if query_on.range_key is None:
        fail_bad_hash(query_on)

    # If the model or index has a range key, the condition can
    # still be (hash key condition AND range key condition)

    if not isinstance(key, And):
        # Too many options to fit into a useful error message.
        fail_bad_range(query_on)

    # This intentionally disallows an AND with just one hash key condition.
    # Otherwise we get into unpacking arbitrarily nested conditions.
    if len(key) != 2:
        fail_bad_range(query_on)
    first_key, second_key = key.conditions

    # Only two options left -- just try both.
    if check_hash_key(query_on, first_key) and check_range_key(query_on, second_key):
        return
    if check_range_key(query_on, first_key) and check_hash_key(query_on, second_key):
        return

    # Nothing else is valid.
    fail_bad_range(query_on)


def check_hash_key(query_on, key):
    """Only allows Comparison("==") against query_on.hash_key"""
    return (
        isinstance(key, Comparison) and
        (key.comparator == "==") and
        (key.column is query_on.hash_key)
    )


def check_range_key(query_on, key):
    """BeginsWith, Between, or any Comparison except '!=' against query_on.range_key"""
    return (
        isinstance(key, (BeginsWith, Between)) or
        (isinstance(key, Comparison) and key.comparator != "!=")
    ) and key.column is query_on.range_key


def fail_bad_hash(query_on):
    msg = "The key condition for a Query on {!r} must be `{} == value`."
    raise InvalidKeyCondition(msg.format(
        simple_query(query_on), simple_column_name(query_on.hash_key)))


def fail_bad_range(query_on):
    msg = "Invalid key condition for a Query on {!r}."
    raise InvalidKeyCondition(msg.format(simple_query(query_on)))


def simple_query(query_on):
    # Model.Meta -> Model
    if getattr(query_on, "__name__", "") == "Meta":
        return query_on.model
    # Index -> Index
    return query_on


def simple_column_name(column):
    return "{}.{}".format(column.model.__name__, column.model_name)
