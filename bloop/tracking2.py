import bloop.condition
_ALL = object()
_TRACKING_ATTR_NAME = "__tracking2"


def _tracking(obj):
    """
    Returns the set used to track changes for a given object.
    If the obj does not have a tracking set, creates and returns one.
    """
    tracking = getattr(obj, _TRACKING_ATTR_NAME, None)
    if tracking is None:
        tracking = {"marked": set(), "snapshot": None}
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
    diff = {"SET": [], "REMOVE": []}
    marked = _tracking(obj)["marked"]
    hash_key, range_key = obj.Meta.hash_key, obj.Meta.range_key
    for column in marked:
        if (column is hash_key) or (column is range_key):
            continue
        value = getattr(obj, column.model_name, None)
        if value is not None:
            diff["SET"].append((column, value))
        # None (or missing, an implicit None) expects the
        # value to be empty (missing) in Dynamo.
        else:
            diff["REMOVE"].append(column)
    if not diff["SET"]:
        diff.pop("SET")
    if not diff["REMOVE"]:
        diff.pop("REMOVE")
    return diff


def set_snapshot(obj, engine):
    """Store a condition to expect the currently marked values of the object.

    The value is stored in the tracking set."""
    snapshot = bloop.condition.Condition()
    marked = _tracking(obj)["marked"]
    # Only expect values (or lack of a value) for colummns that have
    # been explicitly set
    for column in sorted(marked, key=lambda col: col.dynamo_name):
        value = getattr(obj, column.model_name, None)
        # Don't try to dump Nones through the typedef
        if value is not None:
            value = engine.dump(column.typedef, value)
        condition = column == value
        # The renderer shouldn't try to dump the value again.
        # We're dumping immediately in case the value is mutable,
        # such as a set or (many) custom data types.
        condition.dumped = True
        snapshot &= condition
    _tracking(obj)["snapshot"] = snapshot


def get_snapshot(obj):
    # TODO: This should return an AND((c is None) for c in Meta)
    # for a model that has never been loaded/saved.  This case
    # needs to be distinguished from a model that HAS been loaded/saved,
    # but not in an atomic context.  The former has a valid atomic
    # condition (everything None) where the latter did not persist a
    # valid atomic condition because of the engine's mode.
    return _tracking(obj)["snapshot"]


def clear_snapshot(obj):
    """Store a snapshot of an entirely empty object.

    Usually called after deleting an object.
    """
    snapshot = bloop.condition.Condition()
    for column in sorted(obj.Meta.columns, key=lambda col: col.dynamo_name):
        snapshot &= (column is None)
    _tracking(obj)["snapshot"] = snapshot
