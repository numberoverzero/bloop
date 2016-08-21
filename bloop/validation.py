from .exceptions import (
    AbstractModelError,
    InvalidModel,
    UnboundModel,
    UnknownType,
)
from .models import ModelMetaclass


def validate_not_abstract(*objs):
    for obj in objs:
        if obj.Meta.abstract:
            cls = obj if isinstance(obj, type) else obj.__class__
            raise AbstractModelError("{!r} is abstract.".format(cls.__name__))


def validate_is_model(model):
    if not isinstance(model, ModelMetaclass):
        cls = model if isinstance(model, type) else model.__class__
        raise InvalidModel("{!r} does not subclass BaseModel.".format(cls.__name__))


def fail_unknown(model, from_declare):
    # Best-effort check for a more helpful message
    if isinstance(model, ModelMetaclass):
        msg = "{!r} is not bound.  Did you forget to call engine.bind?"
        raise UnboundModel(msg.format(model.__name__)) from from_declare
    else:
        msg = "{!r} is not a registered Type."
        obj = model.__name__ if hasattr(model, "__name__") else model
        raise UnknownType(msg.format(obj)) from from_declare
