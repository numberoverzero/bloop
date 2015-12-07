import collections.abc
import declare


def _validate_projection(projection):
    invalid = ValueError("Index projections must be either 'keys_only', 'all',"
                         " or an iterable of model attributes to include.")
    # String check first since it is also an Iterable
    if isinstance(projection, str):
        projection = projection.upper()
        if projection not in ["KEYS_ONLY", "ALL"]:
            raise invalid
    elif isinstance(projection, collections.abc.Iterable):
        projection = list(projection)
        for attribute in projection:
            if not isinstance(attribute, str):
                raise invalid
    else:
        raise invalid
    return projection


def _update_non_empty(group, iterable):
    """Cull falsey (None) objects from an iterable before adding to a set"""
    group.update(obj for obj in iterable if obj)


class _Index(declare.Field):
    def __init__(self, hash_key=None, range_key=None,
                 name=None, projection="KEYS_ONLY", **kwargs):
        self.hash_key = hash_key
        self.range_key = range_key
        self._dynamo_name = name
        super().__init__(**kwargs)

        # projection_attributes will be set up in `_bind`
        self.projection = _validate_projection(projection)

    @property
    def dynamo_name(self):
        if self._dynamo_name is None:
            return self.model_name
        return self._dynamo_name

    def _bind(self, columns, model_hash, model_range):
        """Set up hash, range keys and compute projection"""
        # Load the column object by model name
        if self.range_key:
            self.range_key = columns[self.range_key]

        # Compute and cache the projected columns
        projected = self.projection_attributes = set()

        # All projections include keys
        keys = (model_hash, model_range, self.hash_key, self.range_key)
        _update_non_empty(projected, keys)

        if self.projection == "ALL":
            projected.update(columns.values())
        elif self.projection == "KEYS_ONLY":
            # Intentionally blank - keys already added above
            pass
        else:
            # List of column model_names - convert to `bloop.Column`
            # objects and merge with keys in projection_attributes
            attrs = (columns[attr] for attr in self.projection)
            projected.update(attrs)
            self.projection = "INCLUDE"

    # TODO: disallow set/get/del for an index.  Raise RuntimeError.


class GlobalSecondaryIndex(_Index):
    def __init__(self, write_units=1, read_units=1, **kwargs):
        if "hash_key" not in kwargs:
            raise ValueError(
                "Must specify a hash_key for a GlobalSecondaryIndex")
        super().__init__(**kwargs)
        self.write_units = write_units
        self.read_units = read_units

    def _bind(self, columns, model_hash, model_range):
        """Load the hash column object by model name"""
        self.hash_key = columns[self.hash_key]
        super()._bind(columns, model_hash, model_range)


class LocalSecondaryIndex(_Index):
    """ LSIs don't have individual read/write units """
    def __init__(self, **kwargs):
        # Hash key MUST be the table hash, pop any other values
        if "hash_key" in kwargs:
            raise ValueError(
                "Can't specify the hash_key of a LocalSecondaryIndex")
        if "range_key" not in kwargs:
            raise ValueError(
                "Must specify a range_key for a LocalSecondaryIndex")
        if ("write_units" in kwargs) or ("read_units" in kwargs):
            raise ValueError(
                "A LocalSecondaryIndex does not have its own read/write units")
        super().__init__(**kwargs)

    def _bind(self, columns, model_hash, model_range):
        """Raise if the model doesn't have a range key"""
        if not model_range:
            raise ValueError(
                "Cannot specify a LocalSecondaryIndex "
                "without a table range key")
        self.hash_key = model_hash
        super()._bind(columns, model_hash, model_range)
