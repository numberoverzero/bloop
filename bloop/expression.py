# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax
import bloop.column
import operator
missing = object()


EXPRESSION_KEYS = {
    "condition": "ConditionExpression",
    "filter": "FilterExpression",
    "key": "KeyConditionExpression"
}
ATTR_NAMES = "ExpressionAttributeNames"
ATTR_VALUES = "ExpressionAttributeValues"
SELECT_MODES = {
    "all": "ALL_ATTRIBUTES",
    "projected": "ALL_PROJECTED_ATTRIBUTES",
    "count": "COUNT",
    "specific": "SPECIFIC_ATTRIBUTES"
}


def render(engine, model, condition, mode, legacy=False):
    renderer = ConditionRenderer(engine, model, legacy=legacy)
    return renderer.render(condition, mode=mode)


class ConditionRenderer(object):
    def __init__(self, engine, model, legacy=False):
        self.engine = engine
        self.model = model
        self.legacy = legacy
        self.attr_values = {}
        self.attr_names = {}
        # Reverse index names so we can re-use ExpressionAttributeNames.
        # We don't do the same for ExpressionAttributeValues since they are
        # dicts of {"TYPE": "VALUE"} and would take more space and time to use
        # as keys, as well as less frequently being re-used than names.
        self.name_attr_index = {}
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
    Base class for scans and queries.

    Thread safe.  The functions key, filter, select, ascending, descending,
    and consistent all return copies of the filter object with the expected
    modifications.

    Example:
        f = Filter(engine, mode, model)
        f2 = f.descending
        f3 = f.consistent

        assert not f._consistent
        assert not f2._consistent
        assert f3._consistent

        assert f._forward
        assert not f2._forward
        assert f3._forward

    '''
    def __init__(self, engine, mode, model, index=None):
        self.engine = engine
        self.mode = mode
        self.model = model
        self.index = index

        self._key_condition = None
        self._filter_condition = None
        self._select = "all"
        self._forward = True
        self._consistent = False

        self._attrs_to_get = []

    def copy(self):
        other = Filter(engine=self.engine, mode=self.mode,
                       model=self.model, index=self.index)

        for attr in ["_filter_condition", "_key_condition",
                     "_select", "_forward", "_consistent"]:
            setattr(other, attr, getattr(self, attr))

        other._attrs_to_get = list(self._attrs_to_get)
        other._key_condition = self._key_condition

        return other

    def key(self, condition):
        # AND multiple conditions
        if self._key_condition:
            condition &= self._key_condition

        # a hash condition is always required; a range condition
        # is allowed if the table/index has a range
        if self.index:
            hash_column = self.index.hash_key
            range_column = self.index.range_key
        else:
            hash_column = self.model.hash_key
            range_column = self.model.range_key

        max_conditions = 1
        if range_column:
            max_conditions += 1

        if not condition:
            raise ValueError("At least one key condition (hash) is required")

        # AND is allowed so long as the index we're using allows hash + range
        if isinstance(condition, And):
            if max_conditions < len(condition):
                msg = ("Model or Index only allows {} condition(s) but"
                       " an AND of {} condition(s) was supplied.").format(
                            max_conditions, len(condition))
                raise ValueError(msg)
            # KeyConditions can only use the following:
            # EQ | LE | LT | GE | GT | BEGINS_WITH | BETWEEN
            for subcond in condition.conditions:
                validate_key_condition(subcond)

            columns = set(subcond.column for subcond in condition.conditions)
            # Duplicate column in AND
            if len(columns) < len(condition):
                raise ValueError("Cannot use a hash/range column more"
                                 " than once when specifying KeyConditions")

            if hash_column not in columns:
                raise ValueError("Must specify a hash key")

            # At this point we've got the same number of columns and
            # conditions, and that's less than or equal to the number of
            # allowed conditions for this model/index.

        # Simply validate all other conditions
        else:
            validate_key_condition(condition)

        other = self.copy()
        other._key_condition = condition
        return other

    def filter(self, condition):
        other = self.copy()
        # AND multiple filters
        if other._filter_condition:
            condition &= other._filter_condition
        other._filter_condition = condition
        return other

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
            other = self.copy()
            other._attrs_to_get.clear()
        if mode == "specific":
            other = self.copy()
            other._attrs_to_get.extend(attrs)

        other._select = mode
        return other

    def count(self):
        other = self.select("count")
        result = other.__gen__()
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
            other = self.copy()
            other._forward = True
            return other
        if name == "descending":
            other = self.copy()
            other._forward = False
            return other
        if name == "consistent":
            other = self.copy()
            other._consistent = True
            return other
        return super().__getattr__(name)

    def __gen__(self):
        meta = self.model.__meta__
        kwargs = {
            'TableName': meta['dynamo.table.name'],
            'Select': SELECT_MODES[self._select],
            'ScanIndexForward': self._forward,
            'ConsistentRead': self._consistent
        }

        if not self._key_condition:
            raise ValueError("Must specify at least a hash key condition")

        if self.index:
            kwargs['IndexName'] = self.index.dynamo_name
            if self._consistent and bloop.column.is_global_index(self.index):
                raise ValueError(
                    "Cannot use ConsistentRead with a GlobalSecondaryIndex")

        if self._select == "specific":
            if not self._attrs_to_get:
                raise ValueError(
                    "Must provide attrs to get with 'specific' mode")
            columns = meta['dynamo.columns.by.model_name']
            attrs = [columns[attr].dynamo_name for attr in self._attrs_to_get]
            kwargs['AttributesToGet'] = attrs

        # Render key and filter conditions
        renderer = ConditionRenderer(self.engine, self.model, legacy=False)

        kwargs.update(renderer.render(self._key_condition, mode="key"))
        if self._filter_condition:
            kwargs.update(renderer.render(self._filter_condition,
                                          mode="filter"))

        return self.engine.dynamodb_client.query(**kwargs)
