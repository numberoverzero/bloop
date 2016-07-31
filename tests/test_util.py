from bloop.util import WeakDefaultDictionary, walk_subclasses

import gc


def test_weakref_default_dict():
    """Provides defaultdict behavior for a WeakKeyDictionary"""
    class Object:
        pass

    def counter():
        current = 0
        while True:
            yield current
            current += 1

    weak_dict = WeakDefaultDictionary(counter().__next__)
    objs = [Object() for _ in range(3)]

    for i, obj in enumerate(objs):
        # default_factory is called
        assert weak_dict[obj] == i

    # Interesting: deleting objs[-1] won't work here because the for loop above
    # has a ref to that object stored in the `obj` variable, which gets leaked
    # :(

    del objs[0]
    gc.collect()
    # Properly cleaning up data when gc'd
    assert len(weak_dict) == 2


def test_walk_subclasses():
    class A:
        pass

    class B:  # Not included
        pass

    class C(A):
        pass

    class D(B, C, A):
        pass

    assert set(walk_subclasses(A)) == {A, C, D}
