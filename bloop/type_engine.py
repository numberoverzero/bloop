import inspect
import uuid

from declare import DeclareException, TypeEngine as TE


class TypeEngine(TE):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def unique(cls):
        """Return a unique type engine (using uuid4)"""
        namespace = str(uuid.uuid4())
        return TypeEngine(namespace)

    def _get_typedef_name(self, typedef):
        if not inspect.isclass(typedef):
            _class = typedef.__class__
        else:
            _class = typedef
        return _class.__name__

    def clear(self):
        self.bound_types.clear()
        self.unbound_types.clear()

    def register(self, typedef):
        if self._get_typedef_name(typedef) in self.bound_types:
            return
        if not self.is_compatible(typedef):
            raise ValueError("Incompatible type {} for engine {}".format(typedef, self))

        # Unbound is a list of instances of the class
        if typedef not in self.unbound_types:
            self.unbound_types.add(typedef)
            typedef._register(self)

    def bind(self, **config):
        while self.unbound_types:
            typedef = self.unbound_types.pop()
            try:
                load, dump = typedef.bind(self, **config)
                self.bound_types[self._get_typedef_name(typedef)] = {
                    "load": load, "dump": dump
                }
            except Exception:
                self.unbound_types.add(typedef)
                raise

    def load(self, typedef, value, **kwargs):
        try:
            bound_type = self.bound_types[self._get_typedef_name(typedef)]
        except KeyError:
            raise DeclareException(
                "Can't load unknown type {}".format(typedef))
        else:
            # Don't need to try/catch since load/dump are bound together
            return bound_type["load"](value, **kwargs)

    def dump(self, typedef, value, **kwargs):
        try:
            bound_type = self.bound_types[self._get_typedef_name(typedef)]
        except KeyError:
            raise DeclareException(
                "Can't dump unknown type {}".format(typedef))
        else:
            # Don't need to try/catch since load/dump are bound together
            return bound_type["dump"](value, **kwargs)

    def is_compatible(self, typedef):  # pragma: no cover
        """
        Returns ``true`` if the typedef is compatible with this engine.

        This function should return ``False`` otherwise.  The default
        implementation will always return ``True``.

        """
        return True

    def __contains__(self, typedef):
        return self._get_typedef_name(typedef) in self.bound_types

