import bloop.filter
DEFAULT_CONFIG = {
    'strict': False,
    'prefetch': 0,
    'consistent': False,
    'persist': 'update',
    'atomic': False
}


class EngineConfig:
    def __init__(self, **config):
        all_config = dict(DEFAULT_CONFIG)
        all_config.update(config)
        for key, value in all_config.items():
            setattr(self, key, value)

    def copy(self, **config):
        copy = EngineConfig()
        copy.strict = self.strict
        copy.prefetch = self.prefetch
        copy.consistent = self.consistent
        copy.persist = self.persist
        copy.atomic = self.atomic
        for key, value in config.items():
            setattr(copy, key, value)
        return copy

    @property
    def strict(self):
        return self._strict

    @strict.setter
    def strict(self, value):
        self._strict = bool(value)

    @property
    def prefetch(self):
        """
        Control how many pages are loaded at once during scans/queries.
          "all": the full query will be executed at once.
          = 0: Pages will be loaded on demand.
          > 0: that number of pages will be fetched at a time.
        """
        return self._prefetch

    @prefetch.setter
    def prefetch(self, value):
        self._prefetch = bloop.filter.validate_prefetch(value)

    @property
    def consistent(self):
        return self._consistent

    @consistent.setter
    def consistent(self, value):
        self._consistent = bool(value)

    @property
    def persist(self):
        """
        Control how objects are persisted.  PutItem will completely overwrite
        an existing item, including deleting fields not set on the
        local item.  A query against a GSI with projection KEYS_ONLY will
        not load non-key attributes, and saving it back with PutItem would
        clear all non-key attributes.
        """
        return self._persist

    @persist.setter
    def persist(self, value):
        if value not in ("overwrite", "update"):
            raise ValueError("persist_mode must be `overwrite` or `update`")
        self._persist = value

    @property
    def atomic(self):
        return self._atomic

    @atomic.setter
    def atomic(self, value):
        self._atomic = bool(value)
