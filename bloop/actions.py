# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions
import enum
from typing import Any, Union


class ActionType(enum.Enum):
    """Represents how Dynamo should apply an update."""
    Add = ("ADD", "{name_ref.name} {value_ref.name}", False)
    Delete = ("DELETE", "{name_ref.name} {value_ref.name}", False)
    Remove = ("REMOVE", "{name_ref.name}", True)
    Set = ("SET", "{name_ref.name}={value_ref.name}", True)

    def __init__(self, wire_key: str, fmt: str, nestable: bool):
        self.wire_key = wire_key
        self.fmt = fmt
        self.nestable = nestable

    def render(self, name_ref, value_ref):
        """name_ref, value_ref should be instances of ``bloop.conditions.Reference`` or None"""
        return self.fmt.format(name_ref=name_ref, value_ref=value_ref)

    def new_action(self, value) -> "Action":
        """Convenience function to instantiate an Action with this type"""
        return Action(self, value)


# O(1) __contains__ for Action.__new__
# noinspection PyTypeChecker
_type_set = set(ActionType)


class Action:
    # noinspection PyUnresolvedReferences
    """
    Encapsulates an update value and how Dynamo should apply the update.

    Generally, you will only need to use the ``Action`` class if you are updating an atomic counter (ADD)
    or making additions and deletions from a set (ADD, DELETE).

    You do not need to use an ``Action`` for SET or REMOVE updates.

    .. code-block:: python

        >>> import bloop.actions
        >>> from my_models import Website, User
        >>> user = User()
        >>> website = Website()
        # SET and REMOVE don't need an explicit action
        >>> user.verified = True
        >>> del user.pw_hash
        # ADD and DELETE need explicit actions
        >>> website.view_count = bloop.actions.add(1)
        >>> website.remote_addrs = bloop.actions.delete({"::0", "localhost"})
    """
    def __new__(cls, action_type: ActionType, value):
        if action_type not in _type_set:
            raise ValueError(f"action_type must be one of {_type_set} but was {action_type}")
        return super().__new__(cls)

    def __init__(self, action_type: ActionType, value):
        self.type = action_type
        self.value = value

    def __repr__(self):
        return f"<Action[{self.type.wire_key}={self.value!r}]>"


def unwrap(x: Union[Action, Any]) -> Any:
    """return an action's inner value"""
    if isinstance(x, Action):
        return x.value
    return x


def wrap(x: Any) -> Action:
    """return an action: REMOVE if x is None else SET"""
    if isinstance(x, Action):
        return x
    elif x is None:
        return remove(None)
    return set(x)


def add(value):
    # noinspection PyUnresolvedReferences
    """Create a new ADD action.

        The ADD action only supports Number and Set data types.
        In addition, ADD can only be used on top-level attributes, not nested attributes.

        .. code-block:: pycon

            >>> import bloop.actions
            >>> from my_models import Website
            >>> website = Website(...)
            >>> website.views = bloop.actions.add(1)
            >>> website.remote_addrs = bloop.actions.add({"::0", "localhost"})
        """
    return Action(ActionType.Add, value)


def delete(value):
    # noinspection PyUnresolvedReferences
    """Create a new DELETE action.

    The DELETE action only supports Set data types.
    In addition, DELETE can only be used on top-level attributes, not nested attributes.

    .. code-block:: pycon

        >>> import bloop.actions
        >>> from my_models import Website
        >>> website = Website(...)
        >>> website.remote_addrs = bloop.actions.delete({"::0", "localhost"})
    """
    return Action(ActionType.Delete, value)


def remove(value=None):
    # noinspection PyUnresolvedReferences
    """Create a new REMOVE action.

    Most types automatically create this action when you use ``del obj.some_attr`` or ``obj.some_attr = None``

    .. code-block:: pycon

        >>> import bloop.actions
        >>> from my_models import User
        >>> user = User(...)
        # equivalent
        >>> user.shell = None
        >>> user.shell = bloop.actions.remove(None)
    """
    return Action(ActionType.Remove, value)


def set(value):
    # noinspection PyUnresolvedReferences
    """Create a new SET action.

    Most types automatically create this action when you use ``obj.some_attr = value``

    .. code-block:: pycon

        >>> import bloop.actions
        >>> from my_models import User
        >>> user = User(...)
        # equivalent
        >>> user.shell = "/bin/sh"
        >>> user.shell = bloop.actions.set("/bin/sh")
    """
    return Action(ActionType.Set, value)
