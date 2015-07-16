import bloop.util
import enum

_DIFF = enum.Enum('DIFF', ['SET', 'DEL', 'NOOP'])
_MISSING = bloop.util.Sentinel('MISSING')
_TRACKING_ATTR_NAME = '__tracking__'


def _tracking_dict(obj):
    '''
    Returns the dict used to track changes for a given object.
    If the obj does not have a tracking dict, sets one up and returns it.
    '''
    tracking = getattr(obj, _TRACKING_ATTR_NAME, None)
    if tracking is None:
        tracking = {}
        setattr(obj, _TRACKING_ATTR_NAME, tracking)
    return tracking


def _set_value(obj, name, value):
    '''
    Store the value of an attr in the obj's tracking dict, overwriting
    any existing value.  This marks the attr as having been loaded from
    DynamoDB.

    TODO: Should this use copy.deepcopy()?  Why would someone mutate the value
    before it is passed to the column's typedef for loading?
    '''
    tracking = _tracking_dict(obj)
    tracking[name] = value


def _del_value(obj, name):
    '''
    Delete the value of an attr from the obj's tracking dict.  This marks
    the attr as having not been loaded from DynamoDB, and should only be used
    when the attribute was EXPECTED to be returned, but DID NOT return because
    it was empty.  This should NOT be used when the attribute was NOT loaded,
    such as a query against an Index that does not project all attributes.
    '''
    _tracking_dict(obj).pop(name, None)


def _get_value(obj, name):
    '''
    Returns the value for an attr from the obj's tracking dict, or MISSING if
    there is no value.
    '''
    return _tracking_dict(obj).get(name, _MISSING)


def _get_tracking(obj):
    '''
    Returns a dict of {dynamo_name: value} for a given object.  Attributes not
    set when the object was last loaded are replaced with MISSING.
    '''
    attrs = {}
    for column in obj.Meta.columns:
        attrs[column.dynamo_name] = _get_value(obj, column.dynamo_name)
    return attrs


def _get_current(obj, engine):
    '''
    Returns a dict of {dynamo_name: value} for a given object.  Attributes not
    set on the object are replaced with MISSING.
    '''
    attrs = engine.__dump__(obj.__class__, obj)
    for column in obj.Meta.columns:
        if column.dynamo_name not in attrs:
            attrs[column.dynamo_name] = _MISSING
    return attrs


def _diff_value(current, loaded):
    '''
    _DIFF of two values, where either, neither, or both can be MISSING.
    Returns the _DIFF value that should be applied to the attribute when
    saving back to DynamoDB.

     current  | loaded  | _DIFF
    ----------|---------|-----------
      foo     |    foo  | _DIFF.NOOP
      MISSING | MISSING | _DIFF.NOOP
      MISSING | bar     | _DIFF.DEL
      foo     |    bar  | _DIFF.SET
      foo     | MISSING | _DIFF.SET
    '''
    if bloop.util.ordered(current) == bloop.util.ordered(loaded):
        return _DIFF.NOOP
    elif current is _MISSING:
        return _DIFF.DEL
    else:
        return _DIFF.SET


def diff_obj(obj, engine):
    '''
    Returns a dict of changes to make for a given object, comparing its
    current values to its tracking (last loaded) values.

    The return dict is:

    {
        "SET": [(Column<Foo>, obj.Foo), (Column<Bar>, obj.Bar), ...],
        "DELETE": [Column<Baz>, ...]
    }
    '''
    current = _get_current(obj, engine)
    tracking = _get_tracking(obj)
    diff = {"SET": [], "DELETE": []}

    for column in obj.Meta.columns:
        name = column.dynamo_name
        current_value = current[name]
        tracking_value = tracking[name]
        change = _diff_value(current_value, tracking_value)
        if change is _DIFF.SET:
            diff["SET"].append((column, getattr(obj, column.model_name)))
        elif change is _DIFF.DEL:
            diff["DELETE"].append(column)
        # Don't do anything if it's _DIFF.NOOP
    return diff


def update(obj, attrs, expected):
    '''
    Loading an object by table should expect all columns.
    Loading an object by index should expect all projected columns*.

    * Except when using an LSI and selecting more than the projected columns,
    in which case all should be expected (LSIs will fetch from the table).

    attrs should be a dict {dynamo_name: dumped value}
    expected should be a list of column objects

    set or del attributes from the obj's tracking dict, depending on whether
    they were expected in the return value, and whether they are actually
    present.

     expected | present | change
    ----------|---------|--------
     True     | True    | SET
     True     | False   | DEL
     False    | Either  | NOOP
    '''
    for column in expected:
        name = column.dynamo_name
        value = attrs.get(name, _MISSING)
        if value is _MISSING:
            _del_value(obj, name)
        else:
            _set_value(obj, name, value)


def update_current(obj, engine):
    ''' Set an object's tracking to match the current state. '''
    attrs = _get_current(obj, engine)
    update(obj, attrs, obj.Meta.columns)


def clear(obj):
    ''' Clear all tracking for an object.  Usually after a delete. '''
    update(obj, {}, obj.Meta.columns)
