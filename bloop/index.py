import collections.abc

import declare


__all__ = ["GlobalSecondaryIndex", "Index", "LocalSecondaryIndex"]
INVALID_PROJECTION = ValueError(
    "Index projections must be either 'keys', 'all', or an iterable of model attributes to include.")


def validate_projection(projection):
    # String check first since it is also an Iterable
    if isinstance(projection, str):
        projection = projection.upper()
        if projection not in ["KEYS", "ALL"]:
            raise INVALID_PROJECTION
    elif isinstance(projection, collections.abc.Iterable):
        projection = list(projection)
        for attribute in projection:
            if not isinstance(attribute, str):
                raise INVALID_PROJECTION
    else:
        raise INVALID_PROJECTION
    return projection


class Index(declare.Field):
    def __init__(self, *, projection, hash_key=None, range_key=None, name=None, **kwargs):
        self.model = None
        self.hash_key = hash_key
        self.range_key = range_key
        self._dynamo_name = name
        super().__init__(**kwargs)

        # projection_attributes will be set up in `_bind`
        self.projection = validate_projection(projection)

    @property
    def dynamo_name(self):
        if self._dynamo_name is None:
            return self.model_name
        return self._dynamo_name

    def _bind(self, model):
        """Set up hash, range keys and compute projection"""
        self.model = model

        # Index by model_name so we can replace hash_key, range_key with the proper `bloop.Column` object
        columns = declare.index(model.Meta.columns, "model_name")
        self.hash_key = columns[self.hash_key]
        if self.range_key:
            self.range_key = columns[self.range_key]

        self.keys = {self.hash_key}
        if self.range_key:
            self.keys.add(self.range_key)

        # Compute and cache the projected columns
        projected = self.projection_attributes = set()

        # All projections include model + index keys
        projected.update(model.Meta.keys)
        projected.update(self.keys)

        if self.projection == "ALL":
            projected.update(columns.values())
        elif self.projection == "KEYS":
            self.projection = "KEYS_ONLY"
        else:
            # List of column model_names - convert to `bloop.Column`
            # objects and merge with keys in projection_attributes
            attrs = (columns[attr] for attr in self.projection)
            projected.update(attrs)
            self.projection = "INCLUDE"

    # TODO: disallow set/get/del for an index.  Raise RuntimeError.


class GlobalSecondaryIndex(Index):
    def __init__(self, *,
                 hash_key=None, range_key=None, read_units=1, write_units=1, name=None, projection=None,
                 **kwargs):
        if hash_key is None:
            raise ValueError("Must specify a hash_key for a GlobalSecondaryIndex")
        if projection is None:
            raise INVALID_PROJECTION
        super().__init__(hash_key=hash_key, range_key=range_key, name=name, projection=projection, **kwargs)
        self.write_units = write_units
        self.read_units = read_units


class LocalSecondaryIndex(Index):
    """ LSIs don't have individual read/write units """
    def __init__(self, *, range_key=None, name=None, projection=None, **kwargs):
        # Hash key MUST be the table hash, pop any other values
        if "hash_key" in kwargs:
            raise ValueError("Can't specify the hash_key of a LocalSecondaryIndex")
        if range_key is None:
            raise ValueError("Must specify a range_key for a LocalSecondaryIndex")
        if projection is None:
            raise INVALID_PROJECTION
        if ("write_units" in kwargs) or ("read_units" in kwargs):
            raise ValueError("A LocalSecondaryIndex does not have its own read/write units")
        super().__init__(range_key=range_key, name=name, projection=projection, **kwargs)

    def _bind(self, model):
        """Raise if the model doesn't have a range key"""
        if not model.Meta.range_key:
            raise ValueError("Can't specify a LocalSecondaryIndex on a table without a range key")
        # this is model_name (string) because super()._bind will do the string -> Column lookup
        self.hash_key = model.Meta.hash_key.model_name
        super()._bind(model)

    @property
    def read_units(self):
        return self.model.Meta.read_units

    @read_units.setter
    def read_units(self, value):
        self.model.Meta.read_units = value

    @property
    def write_units(self):
        return self.model.Meta.write_units

    @write_units.setter
    def write_units(self, value):
        self.model.Meta.write_units = value
