import bloop.condition
import declare
import operator


class _ComparisonMixin(object):
    def __hash__(self):
        # With single inheritance this looks stupid, but as a Mixin this
        # ensures we kick hashing back to the other base class so things
        # don't get fucked up, like `set()`.
        return super().__hash__()

    def __eq__(self, value):
        # Special case - None should use function attribute_not_exists
        if value is None:
            return bloop.condition.AttributeExists(self, negate=True)
        comparator = operator.eq
        return bloop.condition.Comparison(self, comparator, value)

    def __ne__(self, value):
        # Special case - None should use function attribute_exists
        if value is None:
            return bloop.condition.AttributeExists(self, negate=False)
        comparator = operator.ne
        return bloop.condition.Comparison(self, comparator, value)

    def __lt__(self, value):
        comparator = operator.lt
        return bloop.condition.Comparison(self, comparator, value)

    def __gt__(self, value):
        comparator = operator.gt
        return bloop.condition.Comparison(self, comparator, value)

    def __le__(self, value):
        comparator = operator.le
        return bloop.condition.Comparison(self, comparator, value)

    def __ge__(self, value):
        comparator = operator.ge
        return bloop.condition.Comparison(self, comparator, value)

    def is_(self, value):
        """ alias for == """
        return self == value

    def is_not(self, value):
        """ alias for != """
        return self != value

    def between(self, lower, upper):
        """ lower <= column.value <= upper """
        return bloop.condition.Between(self, lower, upper)

    def in_(self, values):
        """ column.value in [3, 4, 5] """
        return bloop.condition.In(self, values)

    def begins_with(self, value):
        return bloop.condition.BeginsWith(self, value)

    def contains(self, value):
        return bloop.condition.Contains(self, value)


class Column(declare.Field, _ComparisonMixin):
    def __init__(self, *args, hash_key=None, range_key=None,
                 name=None, **kwargs):
        self.hash_key = hash_key
        self.range_key = range_key
        self._dynamo_name = name
        super().__init__(*args, **kwargs)

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
