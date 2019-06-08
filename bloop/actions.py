import enum
from typing import Any, Union


class ActionType(enum.Enum):
    Add = ("ADD", "{name_ref.name} {value_ref.name}")
    Delete = ("DELETE", "{name_ref.name} {value_ref.name}")
    Remove = ("REMOVE", "{name_ref.name}")
    Set = ("SET", "{name_ref.name}={value_ref.name}")

    def __init__(self, wire_key: str, fmt: str):
        self.wire_key = wire_key
        self.fmt = fmt

    def render(self, name_ref, value_ref):
        return self.fmt.format(name_ref=name_ref, value_ref=value_ref)


# O(1) __contains__ for Action.__new__
# noinspection PyTypeChecker
_type_set = set(ActionType)


class Action:
    def __new__(cls, action_type: ActionType, value):
        if action_type not in _type_set:
            raise ValueError(f"action_type must be one of {_type_set} but was {action_type}")
        return super().__new__(cls)

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
