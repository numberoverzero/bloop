# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax
import operator


EXPRESSION_KEYS = {
    "condition": "ConditionExpression",
    "filter": "FilterExpression",
    "key": "KeyConditionExpression"
}


class ConditionRenderer:
    def __init__(self, engine):
        self.engine = engine
        self.expressions = {}
        self.attr_values = {}
        self.attr_names = {}
        # Reverse index names so we can re-use ExpressionAttributeNames.
        # We don't do the same for ExpressionAttributeValues since they are
        # dicts of {"TYPE": "VALUE"} and would take more space and time to use
        # as keys, as well as less frequently being re-used than names.
        self.name_attr_index = {}
        self.__ref_index = 0

    def value_ref(self, column, value, *, dumped=False, path=None):
        """
        Dumped controls whether the value is already in a dynamo format (True),
        or needs to be dumped through the engine (False).
        """
        ref = ":v{}".format(self.__ref_index)
        self.__ref_index += 1

        if not dumped:
            typedef = column.typedef
            for segment in (path or []):
                typedef = typedef[segment]
            value = self.engine._dump(typedef, value)

        self.attr_values[ref] = value
        return ref

    def _name_ref(self, name):
        # Small optimization to request size for duplicate name refs
        existing_ref = self.name_attr_index.get(name, None)
        if existing_ref:
            return existing_ref

        ref = "#n{}".format(self.__ref_index)
        self.__ref_index += 1
        self.attr_names[ref] = name
        self.name_attr_index[name] = ref
        return ref

    def name_ref(self, column, path=None):
        pieces = [column.dynamo_name]
        pieces.extend(path or [])
        str_pieces = []
        for piece in pieces:
            # List indexes are attached to last path item directly
            if isinstance(piece, int):
                str_pieces[-1] += "[{}]".format(piece)
            # Path keys are attached with a "."
            else:
                str_pieces.append(self._name_ref(piece))
        return ".".join(str_pieces)

    def refs(self, pair):
        """ Return (#n0, #v1) tuple for a given (column, value) pair """
        column, value = pair
        return self.name_ref(column), self.value_ref(column, value)

    def render(self, condition, mode):
        key = EXPRESSION_KEYS[mode]
        rendered_expression = condition.render(self)
        if rendered_expression:
            self.expressions[key] = rendered_expression

    def projection(self, columns):
        names = map(self.name_ref, columns)
        self.expressions["ProjectionExpression"] = ", ".join(names)

    def update(self, attrs):
        if not attrs:
            return
        set_fmt = "{}={}"
        expression = ""
        if attrs.get("SET", None):
            expression += "SET "
            pairs = map(self.refs, attrs["SET"])
            pairs = (set_fmt.format(*pair) for pair in pairs)
            pairs = ", ".join(pairs)
            expression += pairs
        if attrs.get("REMOVE", None):
            expression += " REMOVE "
            names = map(self.name_ref, attrs["REMOVE"])
            names = ", ".join(names)
            expression += names
        self.expressions["UpdateExpression"] = expression.strip()

    @property
    def rendered(self):
        expressions = dict(self.expressions)
        if self.attr_names:
            expressions["ExpressionAttributeNames"] = self.attr_names
        if self.attr_values:
            expressions["ExpressionAttributeValues"] = self.attr_values
        return expressions


class _BaseCondition:
    dumped = False

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


class Condition(_BaseCondition):
    """Empty condition for iteratively building up conditions.

    Example:
        Constructing an AND condition with 3 sub-conditions::

            condition = Condition()
            for value in [1, 2, 3]:
                condition &= Model.field == value

    """
    def __and__(self, other):
        return other
    __iand__ = __and__

    def __or__(self, other):
        return other
    __ior__ = __or__

    def __invert__(self):
        return self
    __neg__ = __invert__

    def __len__(self):
        return 0

    def __str__(self):  # pragma: no cover
        return "EmptyCondition()"
    __repr__ = __str__

    def render(self, renderer):
        return None


class _MultiCondition(_BaseCondition):
    def __init__(self, *conditions):
        self.conditions = list(conditions)

    def __str__(self):  # pragma: no cover
        conditions = ", ".join(str(c) for c in self.conditions)
        return self.name + "({})".format(conditions)
    __repr__ = __str__

    def __len__(self):
        return sum(map(len, self.conditions))

    def render(self, renderer):
        if len(self.conditions) == 1:
            return self.conditions[0].render(renderer)
        rendered_conditions = (c.render(renderer) for c in self.conditions)
        conjunction = " {} ".format(self.uname)
        return "(" + conjunction.join(rendered_conditions) + ")"


class And(_MultiCondition):
    name = "And"
    uname = "AND"

    def __and__(self, other):
        self.conditions.append(other)
        return self
    __iand__ = __and__


class Or(_MultiCondition):
    name = "Or"
    uname = "OR"

    def __or__(self, other):
        self.conditions.append(other)
        return self
    __ior__ = __or__


class Not(_BaseCondition):
    def __init__(self, condition):
        self.condition = condition

    def __str__(self):  # pragma: no cover
        return "Not({})".format(self.condition)
    __repr__ = __str__

    def __len__(self):
        return len(self.condition)

    def render(self, renderer):
        return "(NOT {})".format(self.condition.render(renderer))


class Comparison(_BaseCondition):
    comparator_strings = {
        operator.eq: "=",
        operator.ne: "<>",
        operator.lt: "<",
        operator.gt: ">",
        operator.le: "<=",
        operator.ge: ">=",
    }

    def __init__(self, column, comparator, value, path=None):
        if comparator not in self.comparator_strings:
            raise ValueError("Unknown comparator '{}'".format(comparator))
        self.column = column
        self.comparator = comparator
        self.value = value
        self.path = path

    def __str__(self):  # pragma: no cover
        return "Compare({}(path={}), {}, {})".format(
            self.column, self.path, self.comparator_strings[self.comparator],
            self.value)
    __repr__ = __str__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref = renderer.value_ref(self.column, self.value,
                                  dumped=self.dumped, path=self.path)
        comparator = self.comparator_strings[self.comparator]
        return "({} {} {})".format(nref, comparator, vref)


class AttributeExists(_BaseCondition):
    def __init__(self, column, negate, path=None):
        self.column = column
        self.negate = negate
        self.path = path

    def __str__(self):  # pragma: no cover
        name = "AttributeNotExists" if self.negate else "AttributeExists"
        return "{}({}(path={}))".format(name, self.column, self.path)
    __repr__ = __str__

    def render(self, renderer):
        name = "attribute_not_exists" if self.negate else "attribute_exists"
        nref = renderer.name_ref(self.column, path=self.path)
        return "({}({}))".format(name, nref)


class BeginsWith(_BaseCondition):
    def __init__(self, column, value, path=None):
        self.column = column
        self.value = value
        self.path = path

    def __str__(self):  # pragma: no cover
        return "BeginsWith({}(path={}), {})".format(
            self.column, self.path, self.value)
    __repr__ = __str__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref = renderer.value_ref(self.column, self.value,
                                  dumped=self.dumped, path=self.path)
        return "(begins_with({}, {}))".format(nref, vref)


class Contains(_BaseCondition):
    def __init__(self, column, value, path=None):
        self.column = column
        self.value = value
        self.path = path

    def __str__(self):  # pragma: no cover
        return "Contains({}(path={}), {})".format(
            self.column, self.path, self.value)
    __repr__ = __str__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref = renderer.value_ref(self.column, self.value,
                                  dumped=self.dumped, path=self.path)
        return "(contains({}, {}))".format(nref, vref)


class Between(_BaseCondition):
    def __init__(self, column, lower, upper, path=None):
        self.column = column
        self.lower = lower
        self.upper = upper
        self.path = path

    def __str__(self):  # pragma: no cover
        return "Between({}(path={}), {}, {})".format(
            self.column, self.path, self.lower, self.upper)
    __repr__ = __str__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref_lower = renderer.value_ref(self.column, self.lower,
                                        dumped=self.dumped, path=self.path)
        vref_upper = renderer.value_ref(self.column, self.upper,
                                        dumped=self.dumped, path=self.path)
        return "({} BETWEEN {} AND {})".format(
            nref, vref_lower, vref_upper)


class In(_BaseCondition):
    def __init__(self, column, values, path=None):
        self.column = column
        self.values = values
        self.path = path

    def __str__(self):  # pragma: no cover
        values = ", ".join(str(c) for c in self.values)
        return "In({}(path={}), [{}])".format(self.column, self.path, values)
    __repr__ = __str__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        values = []
        for value in self.values:
            rendered_value = renderer.value_ref(
                self.column, value, dumped=self.dumped, path=self.path)
            values.append(rendered_value)
        values = ", ".join(values)
        return "({} IN ({}))".format(nref, values)
