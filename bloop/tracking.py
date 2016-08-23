from .conditions import Condition
from .util import WeakDefaultDictionary, signal


# Tracks the state of instances of models:
# 1) Are any columns marked for including in an update?
# 2) Latest snapshot for atomic operations
_obj_tracking = WeakDefaultDictionary(lambda: {"marked": set(), "snapshot": None})


# Watched signals
object_loaded = signal("object_loaded")
object_saved = signal("object_saved")
object_deleted = signal("object_deleted")
object_modified = signal("object_modified")

# Ensure signals aren't connected twice
__signals_connected = False
if not __signals_connected:  # pragma: no branch
    __signals_connected = True

    @object_deleted.connect
    def on_object_deleted(_, obj, **kwargs):
        clear(obj)

    @object_loaded.connect
    def on_object_loaded(engine, obj, **kwargs):
        sync(obj, engine)

    @object_modified.connect
    def on_object_modified(_, obj, column, **kwargs):
        mark(obj, column)

    @object_saved.connect
    def on_object_saved(engine, obj, **kwargs):
        sync(obj, engine)


def clear(obj):
    """Store a snapshot of an entirely empty object.

    Usually called after deleting an object.
    """
    snapshot = Condition()
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
    snapshot = Condition()
    # Only expect values (or lack of a value) for columns that have been explicitly set
    for column in sorted(_obj_tracking[obj]["marked"], key=lambda col: col.dynamo_name):
        value = getattr(obj, column.model_name, None)
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


def get_marked(obj):
    """Returns the set of marked columns for an object"""
    return set(_obj_tracking[obj]["marked"])
