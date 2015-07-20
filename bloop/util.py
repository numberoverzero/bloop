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


class Sentinel:
    def __init__(self, name):
        self.name = name

    def __str__(self):  # pragma: no cover
        return "S({})".format(self.name)
    __repr__ = __str__
