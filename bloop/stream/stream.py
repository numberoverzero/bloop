from ..exceptions import InvalidStream
from ..signals import object_loaded
from ..util import unpack_from_dynamodb
from .coordinator import Coordinator


def stream_for(engine, model):
    if not model.Meta.stream or not model.Meta.stream.get("arn"):
        raise InvalidStream("{!r} does not have a stream arn".format(model))
    coordinator = Coordinator(engine=engine, session=engine.session, stream_arn=model.Meta.stream["arn"])
    stream = Stream(model=model, engine=engine, coordinator=coordinator)
    return stream


class Stream:
    """An iterator over all Records in all Shards in a Stream.

    Ordering is **approximate**.  See :ref:`Stream Internals <internal-streams>` for specific guarantees.

    Basic Usage:

    .. code-block:: python

        stream = engine.stream(Model, position="trim_horizon")
        record = next(stream)
        if record:
            print("{old} became {new}".format(**record))


    Processing in a loop with periodic :func:`heartbeats <bloop.stream.Stream.heartbeat>`:

    .. code-block:: python

        stream = engine.stream(Model, position="trim_horizon")
        next_heartbeat = arrow.now()

        while True:
            process(next(stream))
            if arrow.now() > next_heartbeat:
                next_heartbeat = arrow.now().replace(minutes=12)
                stream.heartbeat()
    """
    def __init__(self, *, model, engine, coordinator):

        #: The :class:`BaseModel` of the Stream to iterate.
        self.model = model

        #: The :class:`Engine` to create instances with.
        self.engine = engine

        #: The :class:`Coordinator` that manages shard iterators and record ordering.
        self.coordinator = coordinator

    def __repr__(self):
        # <Stream[User]>
        return "<{}[{}]>".format(self.__class__.__name__, self.model.__name__)

    def __iter__(self):
        return self

    def __next__(self):
        record = next(self.coordinator)
        if record:
            meta = self.model.Meta
            for key, expected in [("new", meta.columns), ("old", meta.columns), ("key", meta.keys)]:
                if key not in meta.stream["include"]:
                    record[key] = None
                else:
                    self._unpack(record, key, expected)
        return record

    @property
    def token(self):
        """Dict that can be used to reconstruct the current progress of the iterator.

        .. code-block:: python

            with open(".stream-state", "w") as f:
                json.dump(stream.token, f)

            # Some time later
            ...

            with open(".stream-state", "r") as f:
                token = json.load(f)

            stream = engine.stream(MyModel, position=token)
        """
        return self.coordinator.token

    def heartbeat(self):
        """Call periodically to ensure iterators without a fixed sequence number don't expire.

        You should call this once every ~12 minutes so that your "latest" and "trim_horizon" shard iterators don't
        expire.  While iterators have an advertised lifetime of 15 minutes, calling more frequently can avoid
        expiration due to clock skew.

        If an iterator with a sequence number expires, it can be refreshed at the same position.  If an iterator
        at "latest" expires, there's no way to refresh it where it expired; refreshing at "latest" could
        miss records between the expiration position and the new "latest".

        .. code-block:: python

            stream = engine.stream(Model, position="trim_horizon")
            next_heartbeat = arrow.now()

            while True:
                process(next(stream))
                if arrow.now() > next_heartbeat:
                    next_heartbeat = arrow.now().replace(minutes=12)
                    stream.heartbeat()
        """
        self.coordinator.heartbeat()

    def move_to(self, position):
        """Move to either endpoint of the stream, a stream token, or a specific time.

        Moving to "trim_horizon" or "latest" is fast; moving to a point in time is very slow.

        .. code-block:: python

            stream.move_to(stream.token)

            stream.move_to(arrow.now().replace(days=-1))

            stream.move_to("trim_horizon")
        """
        self.coordinator.move_to(position)

    def _unpack(self, record, key, expected):
        """Replaces the attr dict at the given key with an instance of a Model"""
        attrs = record.get(key)
        if attrs is None:
            return
        obj = unpack_from_dynamodb(
            attrs=attrs,
            expected=expected,
            model=self.model,
            engine=self.engine
        )
        object_loaded.send(self.engine, engine=self.engine, obj=obj)
        record[key] = obj
