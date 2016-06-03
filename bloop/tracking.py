import bloop.condition
import bloop.util
import collections

# Tracks the state of instances of models:
# 1) Are any columns marked for including in an update?
# 2) Latest snapshot for atomic operations
# 3) Has this instance ever been synchronized with Dynamo?
_obj_tracking = bloop.util.WeakDefaultDictionary(
    lambda: {"marked": set(), "snapshot": None, "synced": False})

# Tracks the state of models (tables):
# 1) Has the table been created/verified to match the given Meta attributes?
_model_tracking = bloop.util.WeakDefaultDictionary(
    lambda: {"verified": False})


def clear(obj):
    """Store a snapshot of an entirely empty object.

    Usually called after deleting an object.
    """
    _obj_tracking[obj]["synced"] = True
    snapshot = bloop.condition.Condition()
    for column in sorted(obj.Meta.columns, key=lambda col: col.dynamo_name):
        snapshot &= column.is_(None)
    _obj_tracking[obj]["snapshot"] = snapshot


def mark(obj, column):
    """
    Mark a column for a given object as being modified in any way.
    Any marked columns will be pushed (possibly as DELETES) in
    future UpdateItem calls that include the object.
    """
    _obj_tracking[obj]["marked"].add(column)


def sync(obj, engine):
    """Mark the object as having been persisted at least once.

    Store the latest snapshot of all marked values."""
    _obj_tracking[obj]["synced"] = True
    snapshot = bloop.condition.Condition()
    # Only expect values (or lack of a value) for colummns that have
    # been explicitly set
    for column in sorted(_obj_tracking[obj]["marked"],
                         key=lambda col: col.dynamo_name):
        value = getattr(obj, column.model_name, None)
        # Don't try to dump Nones through the typedef
        if value is not None:
            value = engine._dump(column.typedef, value)
        condition = column == value
        # The renderer shouldn't try to dump the value again.
        # We're dumping immediately in case the value is mutable,
        # such as a set or (many) custom data types.
        condition.dumped = True
        snapshot &= condition
    _obj_tracking[obj]["snapshot"] = snapshot


def get_snapshot(obj):
    # Cached value
    condition = _obj_tracking[obj]["snapshot"]
    if condition is not None:
        return condition

    # If the object has never been synced, create and cache
    # a condition that expects every column to be empty
    clear(obj)
    return _obj_tracking[obj]["snapshot"]


def get_update(obj):
    """Creates a dict of changes to make for a given object.

    Returns:
        dict: A dict with two keys "SET" and "REMOVE".

        The dict has the following format::

            {
                "SET": [(Column<Foo>, obj.Foo), (Column<Bar>, obj.Bar), ...],
                "REMOVE": [Column<Baz>, ...]
            }

    """
    diff = collections.defaultdict(list)
    key = set((obj.Meta.hash_key, obj.Meta.range_key))
    for column in _obj_tracking[obj]["marked"]:
        if column in key:
            continue
        value = getattr(obj, column.model_name, None)
        if value is not None:
            diff["SET"].append((column, value))
        # None (or missing, an implicit None) expects the
        # value to be empty (missing) in Dynamo.
        else:
            diff["REMOVE"].append(column)
    return diff


def is_model_verified(model):
    return _model_tracking[model]["verified"]


def verify_model(model):
    _model_tracking[model]["verified"] = True
