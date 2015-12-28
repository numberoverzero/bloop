import weakref


def ordered(obj):
    """
    Return sorted version of nested dicts/lists for comparing.

    http://stackoverflow.com/a/25851972
    """
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


def areinstance(lst, types):
    for obj in lst:
        if not isinstance(obj, types):
            return False
    return True


class WeakDefaultDictionary(weakref.WeakKeyDictionary):
    def __init__(self, default_factory):
        self.default_factory = default_factory
        super().__init__()

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        self[key] = value = self.default_factory()
        return value
