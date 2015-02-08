import bloop.expression
import declare
import uuid
missing = object()
_meta_key = "__column_meta_{}".format(uuid.uuid4().hex)


class Column(declare.Field, bloop.expression.ComparisonMixin):
    def __init__(self, *args, hash_key=None, range_key=None,
                 name=missing, **kwargs):
        self._hash_key = hash_key
        self._range_key = range_key
        self._dynamo_name = name

        self.column_key = "__{}_{}".format(
            self.__class__.__name__, uuid.uuid4().hex)
        super().__init__(*args, **kwargs)

    def __str__(self):
        attrs = ["model_name", "dynamo_name", "hash_key", "range_key"]

        def _attr(attr):
            return "{}={}".format(attr, getattr(self, attr))
        attrs = ", ".join(_attr(attr) for attr in attrs)
        return "Column({})".format(attrs)

    @property
    def hash_key(self):
        return self._hash_key

    @property
    def range_key(self):
        return self._range_key

    @property
    def dynamo_name(self):
        if self._dynamo_name is missing:
            return self.model_name
        return self._dynamo_name

    def __meta__(self, obj):
        ''' Return the column-specific metadata dict for a given object '''
        meta = obj.__dict__.get(_meta_key, None)
        if meta is None:
            meta = obj.__dict__[_meta_key] = {}
        column_meta = meta.get(self.column_key, None)
        if column_meta is None:
            column_meta = meta[self.column_key] = {}
        return column_meta

    def meta_get(self, obj, name, default=missing):
        '''
        look up and return the value of a property in the column metadata,
        setting and return the default value if specified.

        if `default` is not set, KeyError is raised and the metadata dict is
        not mutated.
        '''
        obj_meta = self.__meta__(obj)
        value = obj_meta.get(name, missing)
        # Failed to find - either set and return default, or raise
        if value is missing:
            # Don't mutate on fail to find
            if default is missing:
                raise KeyError("Unknown column meta property {}".format(name))
            else:
                value = obj_meta[name] = default
        return value

    def meta_set(self, obj, name, value):
        obj_meta = self.__meta__(obj)
        obj_meta[name] = value
        return value


class Index(Column):
    def __init__(self, *args, projection='KEYS_ONLY', **kwargs):
        super().__init__(*args, **kwargs)
        self.projection = projection


class GlobalSecondaryIndex(Index):
    def __init__(self, *args, write_units=1, read_units=1, **kwargs):
        super().__init__(*args, **kwargs)
        self.write_units = write_units
        self.read_units = read_units


class LocalSecondaryIndex(Index):
    ''' when constructing a model, you MUST set this index's model attr. '''
    @property
    def hash_key(self):
        hash_column = self.model.__meta__['dynamo.table.hash_key']
        return hash_column.dynamo_name
