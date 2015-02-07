# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax
import operator
missing = object()
EXPRESSION = "ConditionExpression"
ATTR_NAMES = "ExpressionAttributeNames"
ATTR_VALUES = "ExpressionAttributeValues"


def render(engine, model, condition):
    if not condition:
        return {}
    renderer = ConditionRenderer(engine, model)
    condition_expression = condition.render(renderer)

    # An expression contains the compressed string, and any name/value refs
    expression = {EXPRESSION: condition_expression}
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
        return AndCondition(self, other)
    __iand__ = __and__

    def __or__(self, other):
        return OrCondition(self, other)
    __ior__ = __or__

    def __invert__(self):
        return NotCondition(self)
    __neg__ = __invert__


class AndCondition(Condition):
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


class OrCondition(Condition):
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


class NotCondition(Condition):
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
