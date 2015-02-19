# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax
import bloop.column
import operator
missing = object()


EXPRESSION_KEYS = {
    "condition": "ConditionExpression",
    "filter": "FilterExpression"
}
ATTR_NAMES = "ExpressionAttributeNames"
ATTR_VALUES = "ExpressionAttributeValues"
SELECT_MODES = {
    "all": "ALL_ATTRIBUTES",
    "projected": "ALL_PROJECTED_ATTRIBUTES",
    "count": "COUNT",
    "specific": "SPECIFIC_ATTRIBUTES"
}


def is_gsi(index):
    return isinstance(index, bloop.column.GlobalSecondaryIndex)


def render(engine, model, condition, mode="condition"):
    if not condition:
        return {}
    renderer = ConditionRenderer(engine, model)
    rendered_expression = condition.render(renderer)

    # An expression contains the compressed string, and any name/value ref
    key = EXPRESSION_KEYS[mode]
    expression = {key: rendered_expression}
    if renderer.attr_names:
        expression[ATTR_NAMES] = renderer.attr_names
    if renderer.attr_values:
        expression[ATTR_VALUES] = renderer.attr_values
    return expression


class ConditionRenderer(object):
    def __init__(self, engine, model):
        self.engine = engine
        self.model = model
        self.attr_values = {}
        self.attr_names = {}
        self.__ref_index = 0

    def value_ref(self, column, value=missing):
        ref = ":v{}".format(self.__ref_index)
        self.__ref_index += 1

        # Dump the value (default to current) through the column's
        # typedef into dynamo's format, then persist a reference
        # in ExpressionAttributeValues
        type_engine = self.engine.type_engine
        if value is missing:
            value = getattr(self.model, column.model_name)
        dynamo_value = type_engine.dump(column.typedef, value)

        self.attr_values[ref] = dynamo_value
        return ref

    def name_ref(self, column):
        ref = "#n{}".format(self.__ref_index)
        self.__ref_index += 1
        self.attr_names[ref] = column.dynamo_name
        return ref


class Condition(object):
    def __and__(self, other):
        return And(self, other)
    __iand__ = __and__

    def __or__(self, other):
        return Or(self, other)
    __ior__ = __or__

    def __invert__(self):
        return Not(self)
    __neg__ = __invert__


class And(Condition):
    def __init__(self, *conditions):
        self.conditions = conditions

    def __str__(self):
        conditions = ", ".join(str(c) for c in self.conditions)
        return "And({})".format(conditions)

    def render(self, renderer):
        if len(self.conditions) == 1:
            return self.conditions[0].render(renderer)
        rendered_conditions = (c.render(renderer) for c in self.conditions)
        return "(" + " AND ".join(rendered_conditions) + ")"


class Or(Condition):
    def __init__(self, *conditions):
        self.conditions = conditions

    def __str__(self):
        conditions = ", ".join(str(c) for c in self.conditions)
        return "Or({})".format(conditions)

    def render(self, renderer):
        if len(self.conditions) == 1:
            return self.conditions[0].render(renderer)
        rendered_conditions = (c.render(renderer) for c in self.conditions)
        return "(" + " OR ".join(rendered_conditions) + ")"


class Not(Condition):
    def __init__(self, condition):
        self.condition = condition

    def __str__(self):
        return "Not({})".format(self.condition)

    def render(self, renderer):
        return "( NOT {})".format(self.condition.render(renderer))


class Comparison(Condition):
    comparator_strings = {
        operator.eq: "=",
        operator.ne: "<>",
        operator.lt: "<",
        operator.gt: ">",
        operator.le: "<=",
        operator.ge: ">=",
    }

    def __init__(self, column, comparator, value):
        self.column = column
        self.comparator = comparator
        self.value = value

    def __str__(self):
        return "Compare({}, {}, {})".format(
            self.comparator_strings[self.comparator],
            self.column, self.value)

    def render(self, renderer):
        nref = renderer.name_ref(self.column)
        vref = renderer.value_ref(self.column, self.value)
        comparator = self.comparator_strings[self.comparator]
        return "({} {} {})".format(nref, comparator, vref)


class AttributeExists(Condition):
    def __init__(self, column, negate):
        self.column = column
        self.negate = negate

    def __str__(self):
        name = "AttributeNotExists" if self.negate else "AttributeExists"
        return "{}({})".format(name, self.column)

    def render(self, renderer):
        name = "attribute_not_exists" if self.negate else "attribute_exists"
        nref = renderer.name_ref(self.column)
        return "({}({}))".format(name, nref)


class BeginsWith(Condition):
    def __init__(self, column, value):
        self.column = column
        self.value = value

    def __str__(self):
        return "BeginsWith({}, {})".format(self.column, self.value)

    def render(self, renderer):
        nref = renderer.name_ref(self.column)
        vref = renderer.value_ref(self.column, self.value)
        return "(begins_with({}, {}))".format(nref, vref)


class Contains(Condition):
    def __init__(self, column, value):
        self.column = column
        self.value = value

    def __str__(self):
        return "Contains({}, {})".format(self.column, self.value)

    def render(self, renderer):
        nref = renderer.name_ref(self.column)
        vref = renderer.value_ref(self.column, self.value)
        return "(contains({}, {}))".format(nref, vref)


class Between(Condition):
    def __init__(self, column, lower, upper):
        self.column = column
        self.lower = lower
        self.upper = upper

    def __str__(self):
        return "Between({}, {}, {})".format(
            self.column, self.lower, self.upper)

    def render(self, renderer):
        nref = renderer.name_ref(self.column)
        vref_lower = renderer.value_ref(self.column, self.lower)
        vref_upper = renderer.value_ref(self.column, self.upper)
        return "({} BETWEEN {} AND {})".format(nref, vref_lower, vref_upper)


class In(Condition):
    def __init__(self, column, values):
        self.column = column
        self.values = values

    def __str__(self):
        values = ", ".join(str(c) for c in self.values)
        return "In({}, [{}])".format(self.column, values)

    def render(self, renderer):
        nref = renderer.name_ref(self.column)
        values = (renderer.value_ref(self.column, v) for v in self.values)
        values = ", ".join(values)
        return "({} IN ({}))".format(nref, values)


class ComparisonMixin(object):
    def __hash__(self):
        # With single inheritance this looks stupid, but as a Mixin this
        # ensures we kick hashing back to the other base class so things
        # don't get fucked up, like `set()`.
        return super().__hash__()

    def __eq__(self, value):
        # Special case - None should use function attribute_not_exists
        if value is None:
            return AttributeExists(self, negate=True)
        comparator = operator.eq
        return Comparison(self, comparator, value)

    def __ne__(self, value):
        # Special case - None should use function attribute_exists
        if value is None:
            return AttributeExists(self, negate=False)
        comparator = operator.ne
        return Comparison(self, comparator, value)

    def __lt__(self, value):
        comparator = operator.lt
        return Comparison(self, comparator, value)

    def __gt__(self, value):
        comparator = operator.gt
        return Comparison(self, comparator, value)

    def __le__(self, value):
        comparator = operator.le
        return Comparison(self, comparator, value)

    def __ge__(self, value):
        comparator = operator.ge
        return Comparison(self, comparator, value)

    def is_(self, value):
        ''' alias for == '''
        return self == value

    def is_not(self, value):
        ''' alias for != '''
        return self != value

    def between(self, lower, upper):
        ''' lower <= column.value <= upper '''
        return Between(self, lower, upper)

    def in_(self, *values):
        ''' column.value in [3, 4, 5] '''
        return In(self, values)

    def begins_with(self, value):
        return BeginsWith(self, value)

    def contains(self, value):
        return Contains(self, value)


class Filter(object):
    ''' Base class for scans and queries '''
    def __init__(self, engine, mode, model, index=None):
        self.engine = engine
        self.mode = mode
        self.model = model
        self.index = index
        self.condition = None

        self.attrs_to_get = []
        self._select = "all"

        self._forward = True
        self._consistent = False

    def filter(self, condition):
        self.condition = condition
        return self

    def select(self, mode, attrs=None):
        '''
        attrs is REQUIRED when mode is 'specific', and attrs must be a list
        '''
        if mode not in SELECT_MODES:
            msg = "Unknown select mode '{}'.  Must be one of {}"
            raise ValueError(msg.format(mode, list(SELECT_MODES.keys())))
        if mode == "specific" and not attrs:
            raise ValueError("Must provide attrs to get with 'specific' mode")
        if mode == "count":
            self.attrs_to_get.clear()
        if mode == "specific":
            self.attrs_to_get.extend(attrs)

        self._select = mode
        return self

    def count(self):
        self.select("count")
        result = self.__gen__()
        return [result["Count"], result["ScannedCount"]]

    def __iter__(self):
        if self._select == "count":
            raise AttributeError("Cannot iterate COUNT query")
        result = self.__gen__()

        meta = self.model.__meta__
        columns = meta["dynamo.columns"]
        init = meta["bloop.init"]

        for item in result["Items"]:
            attrs = {}
            for column in columns:
                value = item.get(column.dynamo_name, missing)
                if value is not missing:
                    attrs[column.model_name] = value
            yield init(**attrs)

    def __getattr__(self, name):
        if name == "ascending":
            self._forward = True
            return self
        if name == "descending":
            self._forward = False
            return self
        if name == "consistent":
            self._consistent = True
            return self
        return super().__getattr__(name)

    def __gen__(self):
        meta = self.model.__meta__
        kwargs = {
            'TableName': meta['dynamo.table.name'],
            'Select': SELECT_MODES[self.select_mode],
            'ScanIndexForward': self._forward,
            'ConsistentRead': self._consistent
        }
        if self.index:
            kwargs['IndexName'] = self.index.dynamo_name
            if self._consistent and is_gsi(self.index):
                raise ValueError(
                    "Cannot use ConsistentRead with a GlobalSecondaryIndex")

        if self._select == "specific":
            if not self.attrs_to_get:
                raise ValueError(
                    "Must provide attrs to get with 'specific' mode")
            columns = meta['dynamo.columns.by.model_name']
            attrs = [columns[attr].dynamo_name for attr in self.attrs_to_get]
            kwargs['AttributesToGet'] = attrs

        if self.condition:
            condition = render(self.engine, self.model,
                               self.condition, mode="filter")
            kwargs.update(condition)

        # TODO:
        #  KeyConditions

        return self.engine.dynamodb_client.query(**kwargs)
