import enum
from typing import Any, Union


class ActionType(enum.Enum):
    Add = "ADD"
    Delete = "DELETE"
    Set = "SET"
    Remove = "REMOVE"


# O(1) __contains__ for Action.__new__
# noinspection PyTypeChecker
_type_set = set(ActionType)


class Action:
    def __new__(cls, action_type: ActionType, value):
        if action_type not in _type_set:
            raise ValueError(f"action_type must be one of {_type_set} but was {action_type}")
        return super().__new__()

    def __init__(self, action_type: ActionType, value):
        self.type = action_type
        self.value = value

    @staticmethod
    def add(value):
        return Action(ActionType.Add, value)

    @staticmethod
    def delete(value):
        return Action(ActionType.Delete, value)

    @staticmethod
    def set(value):
        return Action(ActionType.Set, value)

    @staticmethod
    def remove(value):
        return Action(ActionType.Remove, value)


def unwrap(x: Union[Action, Any]) -> Any:
    if isinstance(x, Action):
        return x.value
    return x


def wrap(x: Any) -> Action:
    if isinstance(x, Action):
        return x
    elif x is None:
        return Action.remove(None)
    return Action.set(x)


add = Action.add
delete = Action.delete
set = Action.set
remove = Action.remove
