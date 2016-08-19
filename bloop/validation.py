import declare

from .condition import And, BeginsWith, Between, Comparison
from .models import Column, LocalSecondaryIndex
from .exceptions import InvalidKeyCondition, InvalidProjection


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


def validate_search_projection(model, index, projection, strict):
    if not projection:
        raise InvalidProjection("The projection must be 'count', 'all', or a list of Columns to include.")
    if projection == "count":
        return None

    # Table or non-strict LSI
    if not index or (not strict and isinstance(index, LocalSecondaryIndex)):
        available_columns = model.Meta.columns
    # GSI or strict LSI
    else:
        available_columns = index.projected_columns

    if projection == "all":
        return available_columns

    # Keep original around for error messages
    original_projection = projection

    # model_name -> Column
    if all(isinstance(p, str) for p in projection):
        by_model_name = declare.index(model.Meta.columns, "model_name")
        # This could be a list comprehension, but then the
        # user gets a KeyError when they passed a list.  So,
        # do each individually and throw a useful exception.
        converted_projection = []
        for p in projection:
            try:
                converted_projection.append(by_model_name[p])
            except KeyError:
                raise InvalidProjection("{!r} is not a column of {!r}.".format(p, model))
        projection = converted_projection

    # Could have been str/Column mix, or just not Columns.
    if not all(isinstance(p, Column) for p in projection):
        raise InvalidProjection(
            "{!r} is not valid: it must be only Columns or only their model names.".format(original_projection))

    # Must be subset of the available columns
    if set(projection) <= available_columns:
        return projection

    raise InvalidProjection(
        "{!r} includes columns that are not available for {!r}.".format(
            original_projection, simple_query(index or model.Meta)))


def validate_filter_condition(condition, projected_columns):
    if condition is None:
        return
    # Extract columns from condition.  They must
    # be a subset of the projected_columns

    # TODO re-include condition.iter_columns to make this way easier.
    pass


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
