import bloop.util
import gc


def test_weakref_default_dict():
    """Provides defaultdict behavior for a WeakKeyDictionary"""
    index = -1

    class Object:
        pass

    def default_factory():
        nonlocal index
        index += 1
        return index

    d = bloop.util.WeakDefaultDictionary(default_factory)
    objs = [Object() for _ in range(3)]

    for i, obj in enumerate(objs):
        # default_factory is called
        assert d[obj] == i

    # Interesting: deleting objs[-1] won't work here because the for loop above
    # has a ref to that object stored in the `obj` variable, which gets leaked
    # :(

    del objs[0]
    gc.collect()
    # Properly cleaning up data when gc'd
    assert len(d) == 2


def test_walk_subclasses():
    class A:
        pass

    class B:  # Not included
        pass

    class C(A):
        pass

    class D(B, C, A):
        pass

    assert set(bloop.util.walk_subclasses(A)) == set([A, C, D])
