import pytest

from bloop import actions


# noinspection PyTypeChecker
@pytest.mark.parametrize("action_type", list(actions.ActionType))
@pytest.mark.parametrize("value", [None, object(), 1, actions.ActionType.Add, actions.Action])
def test_valid_actions(action_type, value):
    action = actions.Action(action_type, value)
    assert action.type is action_type
    assert action.value is value


@pytest.mark.parametrize("action_type", ["set", "ADD", actions.Action, actions.ActionType])
def test_invalid_actions(action_type):
    with pytest.raises(ValueError):
        # noinspection PyTypeChecker
        actions.Action(action_type, 3)


@pytest.mark.parametrize("name, func, type", [
    ("add", actions.add, actions.ActionType.Add),
    ("delete", actions.delete, actions.ActionType.Delete),
    ("set", actions.set, actions.ActionType.Set),
    ("remove", actions.remove, actions.ActionType.Remove),

])
def test_shorthand(name, func, type):
    assert func(3).type is type
    assert getattr(actions.Action, name)(3).type is type


@pytest.mark.parametrize("value", [3, dict(), actions.ActionType, None])
def test_unwrap_non_action(value):
    assert actions.unwrap(value) is value


@pytest.mark.parametrize("value", [3, dict(), actions.ActionType, None])
@pytest.mark.parametrize("action", [actions.add, actions.delete, actions.remove, actions.set])
def test_unwrap_action(value, action):
    assert actions.unwrap(action(value)) is value


@pytest.mark.parametrize("value, type", [
    (3, actions.ActionType.Set),
    (dict(), actions.ActionType.Set),
    (actions.ActionType, actions.ActionType.Set),
    ("", actions.ActionType.Set),

    (None, actions.ActionType.Remove)
])
def test_wrap_non_action(value, type):
    w = actions.wrap(value)
    assert w.value is value
    assert w.type is type


@pytest.mark.parametrize("value", [3, dict(), actions.ActionType, None])
@pytest.mark.parametrize("func", [actions.add, actions.delete, actions.remove, actions.set])
def test_wrap_action(value, func):
    action = func(value)
    assert actions.wrap(action) is action
