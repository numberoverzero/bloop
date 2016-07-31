from .condition import Condition
from .tracking import get_marked, get_snapshot


__all__ = ["render"]

EXPRESSION_KEYS = {
    "condition": "ConditionExpression",
    "filter": "FilterExpression",
    "key": "KeyConditionExpression"
}


def render(engine, filter=None, select=None, key=None, atomic=None, condition=None, update=None):
    renderer = ConditionRenderer(engine)
    if filter is not None:
        renderer.render(filter, "filter")
    if select is not None:
        renderer.projection(select)
    if key is not None:
        renderer.render(key, "key")
    if (atomic is not None) or (condition is not None):
        if condition is None:
            condition = Condition()
        if atomic is not None:
            condition &= get_snapshot(atomic)
        renderer.render(condition, "condition")
    if update is not None:
        renderer.update_for(update)
    return renderer.rendered


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

    def update_for(self, obj):
        key = {obj.Meta.hash_key, obj.Meta.range_key}
        non_key_columns = filter(lambda c: c not in key, get_marked(obj))
        to_set = []
        to_remove = []
        for column in non_key_columns:
            value = getattr(obj, column.model_name, None)
            if value is None:
                to_remove.append(column)
            else:
                to_set.append((column, value))
        to_set = ["{}={}".format(*self.refs(pair)) for pair in to_set]
        to_remove = [self.name_ref(column) for column in to_remove]
        expression = ""
        if to_set:
            expression += "SET " + ", ".join(to_set)
        if to_remove:
            expression += " REMOVE " + ", ".join(to_remove)
        expression = expression.strip()
        if expression:
            self.expressions["UpdateExpression"] = expression.strip()

    @property
    def rendered(self):
        expressions = dict(self.expressions)
        if self.attr_names:
            expressions["ExpressionAttributeNames"] = self.attr_names
        if self.attr_values:
            expressions["ExpressionAttributeValues"] = self.attr_values
        return expressions
