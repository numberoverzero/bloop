from .condition import Condition
from .tracking import get_marked, get_snapshot


__all__ = ["render"]


def render(engine, filter=None, select=None, key=None, atomic=None, condition=None, update=None):
    renderer = ConditionRenderer(engine)
    if filter is not None:
        renderer.filter_expression(filter)
    if select is not None:
        renderer.projection_expression(select)
    if key is not None:
        renderer.key_expression(key)
    if (atomic is not None) or (condition is not None):
        if condition is None:
            condition = Condition()
        if atomic is not None:
            condition &= get_snapshot(atomic)
        renderer.condition_expression(condition)
    if update is not None:
        renderer.update_expression(update)
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

    def condition_expression(self, condition):
        self.expressions["ConditionExpression"] = condition.render(self)

    def filter_expression(self, condition):
        self.expressions["FilterExpression"] = condition.render(self)

    def key_expression(self, condition):
        self.expressions["KeyConditionExpression"] = condition.render(self)

    def projection_expression(self, columns):
        self.expressions["ProjectionExpression"] = ", ".join(map(self.name_ref, columns))

    def update_expression(self, obj):
        updates = {
            "set": [],
            "remove": []}
        for column in sorted(
                # Don't include key columns in an UpdateExpression
                filter(lambda c: c not in obj.Meta.keys, get_marked(obj)),
                key=lambda c: c.dynamo_name):
            nref = self.name_ref(column)
            value = getattr(obj, column.model_name, None)
            value = self.engine._dump(column.typedef, value)

            if value is not None:
                vref = self.value_ref(column, value, dumped=True)
                updates["set"].append("{}={}".format(nref, vref))
            else:
                updates["remove"].append(nref)

        expression = ""
        if updates["set"]:
            expression += "SET " + ", ".join(updates["set"])
        if updates["remove"]:
            expression += " REMOVE " + ", ".join(updates["remove"])
        if expression:
            self.expressions["UpdateExpression"] = expression.strip()

    @property
    def rendered(self):
        expressions = {k: v for (k, v) in self.expressions.items() if v is not None}
        if self.attr_names:
            expressions["ExpressionAttributeNames"] = self.attr_names
        if self.attr_values:
            expressions["ExpressionAttributeValues"] = self.attr_values
        return expressions
