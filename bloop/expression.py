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


def render(engine, model, condition, mode="condition", legacy=False):
    if not condition:
        return {}
    renderer = ConditionRenderer(engine, model, legacy=legacy)
    rendered_expression = condition.render(renderer)
    # Legacy expressions are of the form:
    # { "dynamo_name":
    #      { "ComparisonOperator": "OPERATOR",
    #        "AttributeValueList": [
    #          {
    #               "S": "20130101",
    #          }, ...]
    #      }
    # }
    if legacy:
        return rendered_expression

    # An expression contains the compressed string, and any name/value ref
    key = EXPRESSION_KEYS[mode]
    expression = {key: rendered_expression}
    if renderer.attr_names:
        expression[ATTR_NAMES] = renderer.attr_names
    if renderer.attr_values:
        expression[ATTR_VALUES] = renderer.attr_values
    return expression


class ConditionRenderer(object):
    def __init__(self, engine, model, legacy=False):
        self.engine = engine
        self.model = model
        self.legacy = legacy
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

        # Legacy renderers don't use ExpressionAttributeValues
        # just return the column dump of the given value
        if self.legacy:
            return dynamo_value
        else:
            return ref

    def name_ref(self, column):
        if self.legacy:
            raise ValueError("Legacy rendering shouldn't need name refs!")
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
        if renderer.legacy:
            raise ValueError("Don't know how to render legacy AND")
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
        if renderer.legacy:
            raise ValueError("Don't know how to render legacy OR")
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


def validate_key_condition(condition):
    if isinstance(condition, BeginsWith):
        return True
    elif isinstance(condition, Between):
        return True
    elif isinstance(condition, Comparison):
        # Valid comparators are EG | LE | LT | GE | GT -- not NE
        return condition.comparator is not operator.ne
    raise ValueError("Invalid KeyCondition {}".format(condition))


class Filter(object):
    '''
    TODO: make thread safe.  The functions key, filter, select,
    ascending, descending, and consistent all return references to the same
    Filter object.  For thread safety, they should return a copy with the
    updated values.
    '''
    valid_range_key_conditions = [Comparison, BeginsWith, Between]

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
        self._key_conditions = {}

    def key(self, *conditions):
        # a hash condition is always required; a range condition
        # is allowed if the table/index has a range
        if self.index:
            # TODO: this meta inspection should be refactored into
            # a transformation library that can inspect model/index
            # pairs generally, instead of relying on something like
            # expression.Filter.key to know how the fields in
            # model.ModelMetaclass.__meta__ are structured
            hash_column = self.index.hash_key
            range_column = self.index.range_key
        else:
            hash_column = self.model.hash_key
            range_column = self.model.range_key

        max_conditions = 1
        if range_column:
            max_conditions += 1

        if not conditions:
            raise ValueError("At least one key condition (hash) is required")
        if len(conditions) > max_conditions:
            msg = "At most {} key conditions can be specified; got {} instead."
            raise ValueError(msg.format(max_conditions, len(conditions)))

        hash_condition = None
        range_condition = None

        # KeyConditions can only use the ComparisonOperators
        # EQ | LE | LT | GE | GT | BEGINS_WITH | BETWEEN
        for condition in conditions:
            validate_key_condition(condition)
            column = condition.column
            if column is hash_column:
                if hash_condition:
                    raise ValueError("HashKey over-specified")
                else:
                    hash_condition = render(self.engine,
                                            self.model, condition,
                                            legacy=True)
            elif column is range_column:
                if range_condition:
                    raise ValueError("RangeKey over-specified")
                else:
                    range_condition = render(self.engine,
                                             self.model, condition,
                                             legacy=True)
            else:
                msg = "Column {} is not a hash or range key".format(column)
                if self.index:
                    msg += " for the index {}".format(self.index.model_name)
                raise ValueError(msg)
        if not hash_condition:
            raise ValueError("Must specify a hash key")

        self._key_conditions = {}
        self._key_conditions.update(hash_condition)
        if range_condition:
            self._key_conditions.update(range_condition)
        return self

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
                    value = self.engine.__load__(column.typedef, value)
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
            'Select': SELECT_MODES[self._select],
            'ScanIndexForward': self._forward,
            'ConsistentRead': self._consistent,
            'KeyConditions': self._key_conditions
        }

        if not self._key_conditions:
            raise ValueError("Must specify at least a hash key condition")
        if self.index:
            kwargs['IndexName'] = self.index.dynamo_name
            if self._consistent and bloop.column.is_global_index(self.index):
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

        return self.engine.dynamodb_client.query(**kwargs)
