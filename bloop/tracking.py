import bloop.condition
import collections
_ALL = object()
_TRACKING_ATTR_NAME = "__tracking"


def _tracking(obj):
    """
    Returns the set used to track changes for a given object.
    If the obj does not have a tracking set, creates and returns one.
    """
    tracking = getattr(obj, _TRACKING_ATTR_NAME, None)
    if tracking is None:
        tracking = {"marked": set(), "snapshot": None, "synced": False}
        setattr(obj, _TRACKING_ATTR_NAME, tracking)
    return tracking


def mark(obj, column):
    """
    Mark a column for a given object as being modified in any way.
    Any marked columns will be pushed (possibly as DELETES) in
    future UpdateItem calls that include the object.
    """
    _tracking(obj)["marked"].add(column)


def dump_update(obj):
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
    marked = _tracking(obj)["marked"]
    hash_key, range_key = obj.Meta.hash_key, obj.Meta.range_key
    for column in marked:
        # The next two lines have pragmas even though there are tests to
        # exercise them, because coverage does not observe the continuation.
        # A simple verification is to add a line before continue that is
        # simply ... and remove the pragmas.  For tests that cover these lines,
        # see:
        #   test_tracking.py/test_dump_update
        #   test_engine.py/test_save_set_del_field
        if (column is hash_key) or (column is range_key):  # pragma: no branch
            continue  # pragma: no cover

        value = getattr(obj, column.model_name, None)
        if value is not None:
            diff["SET"].append((column, value))
        # None (or missing, an implicit None) expects the
        # value to be empty (missing) in Dynamo.
        else:
            diff["REMOVE"].append(column)
    return diff


def set_synced(obj):
    """Mark the object as having been persisted at least once."""
    _tracking(obj)["synced"] = True


def set_snapshot(obj, engine):
    """Store a condition to expect the currently marked values of the object.

    The value is stored in the tracking set.
    Nothing is stored if the engine isn't atomic.
    """
    if not engine.config["atomic"]:
        return
    snapshot = bloop.condition.Condition()
    marked = _tracking(obj)["marked"]
    # Only expect values (or lack of a value) for colummns that have
    # been explicitly set
    for column in sorted(marked, key=lambda col: col.dynamo_name):
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
    _tracking(obj)["snapshot"] = snapshot


def get_snapshot(obj, engine):
    # Cached value
    condition = _tracking(obj)["snapshot"]
    if condition is not None:
        return condition

    # If the object has never been synced, create and cache a condition
    # that expects every column to be empty
    if not _tracking(obj)["synced"]:
        clear_snapshot(obj, engine)
        return _tracking(obj)["snapshot"]

    # The object has been synced at least once, and no snapshot was created.
    # That means the object may have been mutated since it was loaded, and
    # no atomic condition can be created against its last loaded values.
    raise RuntimeError((
        "No atomic condition found for {}; was it "
        "loaded through an atomic engine?").format(obj))


def clear_snapshot(obj, engine):
    """Store a snapshot of an entirely empty object.

    Usually called after deleting an object.
    Nothing is stored if the engine isn't atomic.
    """
    if not engine.config["atomic"]:
        _tracking(obj)["snapshot"] = None
        return
    snapshot = bloop.condition.Condition()
    for column in sorted(obj.Meta.columns, key=lambda col: col.dynamo_name):
        snapshot &= column.is_(None)
    _tracking(obj)["snapshot"] = snapshot
