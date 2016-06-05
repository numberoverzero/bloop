import bloop.condition
import bloop.tracking
import declare
import operator


class _ComparisonMixin:
    def __init__(self, *, path=None, obj=None, **kwargs):
        self.path = path or []
        # By default the object points to itself; subclasses and recursive
        # structures (for instance, __getitem__) can specify the original
        # object to maintain constant time access to the underlying object.
        self.__obj = obj or self
        super().__init__(**kwargs)

    def __hash__(self):
        # With single inheritance this looks stupid, but as a Mixin this
        # ensures we kick hashing back to the other base class so things
        # don't get fucked up, like `set()`.

        # While the docs recommend using `__hash__ = some_parent.__hash__`,
        # that won't work here - we don't know the parent when the mixin is
        # defined.
        # https://docs.python.org/3.1/reference/datamodel.html#object.__hash__
        return super().__hash__()

    def __eq__(self, value):
        # Special case - None should use function attribute_not_exists
        if value is None:
            return bloop.condition.AttributeExists(
                self.__obj, negate=True, path=self.path)
        comparator = operator.eq
        return bloop.condition.Comparison(
            self.__obj, comparator, value, path=self.path)
    is_ = __eq__

    def __ne__(self, value):
        # Special case - None should use function attribute_exists
        if value is None:
            return bloop.condition.AttributeExists(
                self.__obj, negate=False, path=self.path)
        comparator = operator.ne
        return bloop.condition.Comparison(
            self.__obj, comparator, value, path=self.path)
    is_not = __ne__

    def __lt__(self, value):
        comparator = operator.lt
        return bloop.condition.Comparison(
            self.__obj, comparator, value, path=self.path)

    def __gt__(self, value):
        comparator = operator.gt
        return bloop.condition.Comparison(
            self.__obj, comparator, value, path=self.path)

    def __le__(self, value):
        comparator = operator.le
        return bloop.condition.Comparison(
            self.__obj, comparator, value, path=self.path)

    def __ge__(self, value):
        comparator = operator.ge
        return bloop.condition.Comparison(
            self.__obj, comparator, value, path=self.path)

    def between(self, lower, upper):
        """ lower <= column.value <= upper """
        return bloop.condition.Between(
            self.__obj, lower, upper, path=self.path)

    def in_(self, values):
        """ column.value in [3, 4, 5] """
        return bloop.condition.In(self.__obj, values, path=self.path)

    def begins_with(self, value):
        return bloop.condition.BeginsWith(self.__obj, value, path=self.path)

    def contains(self, value):
        return bloop.condition.Contains(self.__obj, value, path=self.path)

    def __getitem__(self, path):
        if not isinstance(path, (str, int)):
            raise ValueError("Documents can only be indexed by"
                             " strings or integers.")
        return _ComparisonMixin(obj=self.__obj, path=self.path + [path])


class Column(declare.Field, _ComparisonMixin):
    def __init__(self, typedef, hash_key=None, range_key=None,
                 name=None, **kwargs):
        self.hash_key = hash_key
        self.range_key = range_key
        self._dynamo_name = name
        kwargs['typedef'] = typedef
        super().__init__(**kwargs)

    def __str__(self):  # pragma: no cover
        attrs = ["model_name", "dynamo_name", "hash_key", "range_key"]

        def _attr(attr):
            return "{}={}".format(attr, getattr(self, attr))
        attrs = ", ".join(_attr(attr) for attr in attrs)
        return "{}({})".format(self.__class__.__name__, attrs)
    __repr__ = __str__

    @property
    def dynamo_name(self):
        if self._dynamo_name is None:
            return self.model_name
        return self._dynamo_name

    def set(self, obj, value):
        super().set(obj, value)
        # Notify the tracking engine that this value
        # was intentionally mutated
        bloop.tracking.mark(obj, self)

    def delete(self, obj):
        try:
            super().delete(obj)
        finally:
            # Notify the tracking engine that this value
            # was intentionally mutated
            bloop.tracking.mark(obj, self)
