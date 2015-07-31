import bloop.condition
import bloop.util
import enum

_DIFF = enum.Enum("DIFF", ["SET", "REM", "NOOP"])
_TRACKING_ATTR_NAME = "__tracking"


def _tracking_dict(obj):
    """
    Returns the dict used to track changes for a given object.
    If the obj does not have a tracking dict, sets one up and returns it.
    """
    tracking = getattr(obj, _TRACKING_ATTR_NAME, None)
    if tracking is None:
        tracking = {"values": {}, "loaded": set()}
        setattr(obj, _TRACKING_ATTR_NAME, tracking)
    return tracking


def _set_value(obj, name, value):
    """
    Store the value of an attr in the obj's tracking dict, overwriting
    any existing value.  This marks the attr as having been loaded from
    DynamoDB.

    TODO: Should this use copy.deepcopy()?  Why would someone mutate the value
    before it is passed to the column's typedef for loading?
    """
    _tracking_dict(obj)["values"][name] = value


def _del_value(obj, name):
    """
    Delete the value of an attr from the obj's tracking dict.  This should only
    be called when the attribute was EXPECTED to be returned, but DID NOT
    return because it was empty.  This should NOT be used when the attribute
    was NOT loaded, such as a query against an Index that does not project
    all attributes.
    """
    _tracking_dict(obj)["values"].pop(name, None)


def _get_value(obj, name):
    """
    Returns the value for an attr from the obj's tracking dict.  Raises
    KeyError if there is no value.
    """
    return _tracking_dict(obj)["values"][name]


def _set_loaded(obj, loaded):
    """
    Set the expected columns for the object.  This is used when constructing
    atomic conditions, so that only loaded columns have conditions generated.

    Otherwise, there would be a ``attribute_not_exists`` for every missing
    column, even those not loaded.
    """
    _tracking_dict(obj)["loaded"] = set(loaded)


def _get_loaded(obj):
    """Return the loaded columns for the object."""
    return _tracking_dict(obj)["loaded"]


def _get_tracking(obj):
    """
    Returns a dict of {dynamo_name: value} for a given object.  Attributes not
    set when the object was last loaded are not returned.
    """
    attrs = {}
    for column in obj.Meta.columns:
        try:
            attrs[column.dynamo_name] = _get_value(obj, column.dynamo_name)
        except KeyError:
            continue
    return attrs


def _get_current(obj, engine):
    """
    Returns a dict of {dynamo_name: value} for a given object.  Attributes not
    set on the object are not included.
    """
    attrs = engine._dump(obj.__class__, obj)
    return attrs


def _diff_value(current, loaded):
    """
    _DIFF of two values, where either, neither, or both can be None.
    Returns the _DIFF value that should be applied to the attribute when
    saving back to DynamoDB.

    =======  =======  ==========
    current  loaded   _DIFF
    =======  =======  ==========
    foo      foo      _DIFF.NOOP
    None     None     _DIFF.NOOP
    None     bar      _DIFF.REM
    foo      bar      _DIFF.SET
    foo      None     _DIFF.SET
    =======  =======  ==========

    """
    if bloop.util.ordered(current) == bloop.util.ordered(loaded):
        return _DIFF.NOOP
    elif current is None:
        return _DIFF.REM
    else:
        return _DIFF.SET


def diff_obj(obj, engine):
    """Creates a dict of changes to make for a given object.

    Returns:
        dict: A dict with two keys "SET" and "REMOVE".

        The dict has the following format::

            {
                "SET": [(Column<Foo>, obj.Foo), (Column<Bar>, obj.Bar), ...],
                "REMOVE": [Column<Baz>, ...]
            }

    """
    hash_key, range_key = obj.Meta.hash_key, obj.Meta.range_key
    current = _get_current(obj, engine)
    tracking = _get_tracking(obj)
    diff = {"SET": [], "REMOVE": []}
    for column in obj.Meta.columns:
        # hash and range keys can"t be updated
        if (column is hash_key) or (column is range_key):
            continue
        name = column.dynamo_name
        current_value = current.get(name, None)
        tracking_value = tracking.get(name, None)
        change = _diff_value(current_value, tracking_value)
        if change is _DIFF.SET:
            diff["SET"].append((column, getattr(obj, column.model_name)))
        elif change is _DIFF.REM:
            diff["REMOVE"].append(column)
        # Don"t do anything if it's _DIFF.NOOP
    if not diff["SET"]:
        diff.pop("SET")
    if not diff["REMOVE"]:
        diff.pop("REMOVE")
    return diff


def update(obj, attrs, expected):
    """Update the object's tracking dict.

    The updates are created by the intersection of attrs and expected.

    Loading an object by table should expect all columns.
    Loading an object by index should expect all projected columns\*.

    \* Except when using an LSI and selecting more than the projected columns,
    in which case all should be expected (LSIs will fetch from the table).

    attrs should be a dict {dynamo_name: dumped value}
    expected should be a list of column objects

    set or del attributes from the obj's tracking dict, depending on whether
    they were expected in the return value, and whether they are actually
    present::

         expected | present | change
        ----------|---------|--------
         True     | True    | SET
         True     | False   | REM
         False    | Either  | NOOP

    """
    for column in expected:
        name = column.dynamo_name
        value = attrs.get(name, None)
        if value is None:
            _del_value(obj, name)
        else:
            _set_value(obj, name, value)
    _set_loaded(obj, expected)


def update_current(obj, engine):
    """ Set an object's tracking to match the current state. """
    attrs = _get_current(obj, engine)
    update(obj, attrs, obj.Meta.columns)


def clear(obj):
    """ Clear all tracking for an object.  Usually after a delete. """
    update(obj, {}, obj.Meta.columns)


def atomic_condition(obj):
    """
    Generate a condition to expect the last loaded state of an object.
    Missing fields will expect `is_(None)`

    TODO: this will expect attribute_not_exists for columns that haven't been
          loaded, even if they did exist on last load (for instance, loading)
          through a key_only projection would give us no non-key attributes,
          but this would produce an expectation that those attributes were
          actually not set.
    """
    atomic = bloop.condition.Condition()
    tracking = _get_tracking(obj)
    loaded = _get_loaded(obj)

    # While sorting isn't required, it allows us some sanity in testing.
    # The overhead to do so will rarely be significant.
    for column in sorted(loaded, key=lambda col: col.dynamo_name):
        value = tracking.get(column.dynamo_name, None)
        condition = column.is_(value)
        condition.dumped = True
        atomic &= condition
    return atomic
