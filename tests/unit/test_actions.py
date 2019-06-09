import pytest

from bloop.actions import (
    Action,
    ActionType,
    add,
    delete,
    remove,
    set,
    unwrap,
    wrap,
)
from bloop.conditions import Reference


short_funcs = [add, delete, remove, set]


# noinspection PyTypeChecker
@pytest.mark.parametrize("action_type", ActionType)
@pytest.mark.parametrize("value", [None, object(), 1, ActionType.Add, Action])
def test_valid_actions(action_type, value):
    action = Action(action_type, value)
    assert action.type is action_type
    assert action.value is value


@pytest.mark.parametrize("action_type", ["set", "ADD", Action, ActionType])
def test_invalid_actions(action_type):
    with pytest.raises(ValueError):
        # noinspection PyTypeChecker
        Action(action_type, 3)


@pytest.mark.parametrize("action_type, expected", [
    (ActionType.Add, "my_name my_value"),
    (ActionType.Delete, "my_name my_value"),
    (ActionType.Remove, "my_name"),
    (ActionType.Set, "my_name=my_value"),
])
def test_render(action_type, expected):
    name_ref = Reference("my_name", object(), object())
    value_ref = Reference("my_value", object(), object())
    assert action_type.render(name_ref, value_ref) == expected


@pytest.mark.parametrize("func, type", [
    (add, ActionType.Add),
    (delete, ActionType.Delete),
    (set, ActionType.Set),
    (remove, ActionType.Remove),

])
def test_shorthand(func, type):
    assert func(3).type is type


@pytest.mark.parametrize("value", [3, dict(), ActionType, None])
def test_unwrap_non_action(value):
    assert unwrap(value) is value


@pytest.mark.parametrize("value", [3, dict(), ActionType, None])
@pytest.mark.parametrize("func", short_funcs)
def test_unwrap_action(value, func):
    assert unwrap(func(value)) is value


@pytest.mark.parametrize("value, type", [
    (3, ActionType.Set),
    (dict(), ActionType.Set),
    (ActionType, ActionType.Set),
    ("", ActionType.Set),

    (None, ActionType.Remove)
])
def test_wrap_non_action(value, type):
    w = wrap(value)
    assert w.value is value
    assert w.type is type


@pytest.mark.parametrize("value", [3, dict(), ActionType, None])
@pytest.mark.parametrize("func", short_funcs)
def test_wrap_action(value, func):
    action = func(value)
    assert wrap(action) is action


# noinspection PyTypeChecker
@pytest.mark.parametrize("action_type", list(ActionType))
def test_new_action(action_type):
    value = object()
    action = action_type.new_action(value)
    assert isinstance(action, Action)
    assert action.type is action_type
    assert action.value is value


# noinspection PyTypeChecker
@pytest.mark.parametrize("action_type", list(ActionType))
def test_repr(action_type):
    action = action_type.new_action("testval")
    assert repr(action) == f"<Action[{action_type.wire_key}='testval']>"
