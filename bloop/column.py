import declare
import uuid
missing = object()
_meta_key = "__column_meta_{}".format(uuid.uuid4().hex)


class Column(declare.Field):
    def __init__(self, *args, **kwargs):
        self.column_key = "__{}_{}".format(
            self.__class__.__name__, uuid.uuid4().hex)
        super().__init__(*args, **kwargs)

    def meta(self, obj):
        ''' Return the column-specific metadata dict for a given object '''
        meta = obj.__dict__.get(_meta_key, None)
        if meta is None:
            meta = obj.__dict__[_meta_key] = {}
        column_meta = meta.get(self.column_key, None)
        if column_meta is None:
            column_meta = meta[self.column_key] = {}
        return column_meta

    def prop_get(self, obj, name, default=missing):
        '''
        look up and return the value of a property in the column metadata,
        setting and return the default value if specified.

        if `default` is not set, KeyError is raised and the metadata dict is
        not mutated.
        '''
        obj_meta = self.meta(obj)
        value = obj_meta.get(name, missing)
        # Failed to find - either set and return default, or raise
        if value is missing:
            # Don't mutate on fail to find
            if default is missing:
                raise KeyError("Unknown column meta property {}".format(name))
            else:
                value = obj_meta[name] = default
        return value

    def prop_set(self, obj, name, value):
        obj_meta = self.meta(obj)
        obj_meta[name] = value
        return value


class DirtyColumn(Column):
    def is_dirty(self, obj):
        return self.prop_get(obj, 'dirty', default=False)

    def mark_dirty(self, obj):
        return self.prop_set(obj, 'dirty', True)

    def set(self, obj, value):
        if not self.is_dirty(obj):
            try:
                original = self.get(obj)
            except AttributeError:
                # If we couldn't load before, must be a mutation
                self.mark_dirty(obj)
            else:
                # If this value is different it's a mutation
                if value != original:
                    self.mark_dirty(obj)
        super().set(obj, value)

    def delete(self, obj):
        try:
            super().delete(obj)
        except AttributeError:
            # If we couldn't delete then it was never set
            raise
        else:
            # Successful deletion is a mutation
            self.mark_dirty(obj)
