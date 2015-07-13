# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax
import operator

missing = object()
EXPRESSION_KEYS = {
    "condition": "ConditionExpression",
    "filter": "FilterExpression",
    "key": "KeyConditionExpression"
}
ATTR_NAMES = "ExpressionAttributeNames"
ATTR_VALUES = "ExpressionAttributeValues"


def render(engine, condition, mode, legacy=False):
    renderer = ConditionRenderer(engine, legacy=legacy)
    return renderer.render(condition, mode=mode)


class ConditionRenderer(object):
    def __init__(self, engine, legacy=False):
        self.engine = engine
        self.legacy = legacy
        self.attr_values = {}
        self.attr_names = {}
        # Reverse index names so we can re-use ExpressionAttributeNames.
        # We don't do the same for ExpressionAttributeValues since they are
        # dicts of {"TYPE": "VALUE"} and would take more space and time to use
        # as keys, as well as less frequently being re-used than names.
        self.name_attr_index = {}
        self.__ref_index = 0

    def value_ref(self, column, value):
        ref = ":v{}".format(self.__ref_index)
        self.__ref_index += 1

        # Dump the value (default to current) through the column's
        # typedef into dynamo's format, then persist a reference
        # in ExpressionAttributeValues
        type_engine = self.engine.type_engine
        dynamo_value = type_engine.dump(column.typedef, value)

        self.attr_values[ref] = dynamo_value

        # Legacy renderers don't use ExpressionAttributeValues
        # just return the column dump of the given value
        if self.legacy:
            return dynamo_value
        else:
            return ref

    def name_ref(self, column):
        # Small optimization to request size for duplicate name refs
        existing_ref = self.name_attr_index.get(column.dynamo_name, None)
        if existing_ref:
            return existing_ref

        if self.legacy:
            raise ValueError("Legacy rendering shouldn't need name refs!")

        ref = "#n{}".format(self.__ref_index)
        self.__ref_index += 1
        self.attr_names[ref] = column.dynamo_name
        self.name_attr_index[column.dynamo_name] = ref
        return ref

    def render(self, condition, mode):
        if not condition:
            return {}
        rendered_expression = condition.render(self)
        # Legacy expressions are of the form:
        # { "dynamo_name":
        #      { "ComparisonOperator": "OPERATOR",
        #        "AttributeValueList": [
        #          {
        #               "S": "20130101",
        #          }, ...]
        #      }
        # }
        if self.legacy:
            return rendered_expression

        # An expression contains the compressed string, and any name/value ref
        key = EXPRESSION_KEYS[mode]
        expression = {key: rendered_expression}
        if self.attr_names:
            expression[ATTR_NAMES] = self.attr_names
        if self.attr_values:
            expression[ATTR_VALUES] = self.attr_values
        return expression


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

    def __len__(self):
        return 1


class MultiCondition(Condition):
    def __init__(self, *conditions):
        self.conditions = conditions

    def __str__(self):
        conditions = ", ".join(str(c) for c in self.conditions)
        return self.name + "({})".format(conditions)

    def __len__(self):
        return sum(map(len, self.conditions))

    def render(self, renderer):
        if renderer.legacy:
            raise ValueError("Don't know how to render legacy " + self.uname)
        if len(self.conditions) == 1:
            return self.conditions[0].render(renderer)
        rendered_conditions = (c.render(renderer) for c in self.conditions)
        conjunction = " {} ".format(self.uname)
        return "(" + conjunction.join(rendered_conditions) + ")"


class And(MultiCondition):
    name = "And"
    uname = "AND"


class Or(MultiCondition):
    name = "Or"
    uname = "OR"


class Not(Condition):
    def __init__(self, condition):
        self.condition = condition

    def __str__(self):
        return "Not({})".format(self.condition)

    def __len__(self):
        return len(self.condition)

    def render(self, renderer):
        if renderer.legacy:
            raise ValueError("Don't know how to render legacy NOT")
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
    legacy_strings = {
        operator.eq: "EQ",
        operator.ne: "NE",
        operator.lt: "LT",
        operator.gt: "GT",
        operator.le: "LE",
        operator.ge: "GE",
    }

    def __init__(self, column, comparator, value):
        if comparator not in self.comparator_strings:
            raise ValueError("Unknown comparator '{}'".format(comparator))
        self.column = column
        self.comparator = comparator
        self.value = value

    def __str__(self):
        return "Compare({}, {}, {})".format(
            self.comparator_strings[self.comparator],
            self.column, self.value)

    def render(self, renderer):
        if renderer.legacy:
            return {self.column.dynamo_name: {
                    "ComparisonOperator": self.legacy_strings[self.comparator],
                    "AttributeValueList": [
                        renderer.value_ref(self.column, self.value)
                    ]}}
        else:
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
        if renderer.legacy:
            raise ValueError("Don't know how to render legacy AttributeExists")
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
        if renderer.legacy:
            return {self.column.dynamo_name: {
                    "ComparisonOperator": "BEGINS_WITH",
                    "AttributeValueList": [
                        renderer.value_ref(self.column, self.value)
                    ]}}
        else:
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
        if renderer.legacy:
            raise ValueError("Don't know how to render legacy Contains")
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
        if renderer.legacy:
            return {self.column.dynamo_name: {
                    "ComparisonOperator": "BETWEEN",
                    "AttributeValueList": [
                        renderer.value_ref(self.column, self.lower),
                        renderer.value_ref(self.column, self.upper)
                    ]}}
        else:
            nref = renderer.name_ref(self.column)
            vref_lower = renderer.value_ref(self.column, self.lower)
            vref_upper = renderer.value_ref(self.column, self.upper)
            return "({} BETWEEN {} AND {})".format(
                nref, vref_lower, vref_upper)


class In(Condition):
    def __init__(self, column, values):
        self.column = column
        self.values = values

    def __str__(self):
        values = ", ".join(str(c) for c in self.values)
        return "In({}, [{}])".format(self.column, values)

    def render(self, renderer):
        if renderer.legacy:
            raise ValueError("Don't know how to render legacy IN")
        nref = renderer.name_ref(self.column)
        values = (renderer.value_ref(self.column, v) for v in self.values)
        values = ", ".join(values)
        return "({} IN ({}))".format(nref, values)
