from ..exceptions import InvalidStream
from ..signals import object_loaded
from ..util import unpack_from_dynamodb
from .coordinator import Coordinator


def stream_for(engine, model):
    """Helper to construct a stream for an engine and model.

    .. code-block:: pycon

        >>> from my_project.models import User
        >>> from bloop import Engine, stream_for
        >>> engine = Engine()
        >>> engine.bind(User)
        >>> stream = stream_for(engine, User)
        >>> next(stream)
        {'key': None,
         'old': None,
         'new': User(id=0, email="user@domain.com"),
         'meta': {'created_at': <Arrow [2016-10-28T01:58:00-07:00]>,
                  'event': {'id': '5ad8700c0adbfad0083e44fc2e3861c0',
                            'type': 'insert',
                            'version': '1.1'},
                  'sequence_number': '100000000006486326346'}
        }

    :rtype: :class:`~bloop.stream.Stream`
    """
    if not model.Meta.stream or not model.Meta.stream.get("arn"):
        raise InvalidStream("{!r} does not have a stream arn".format(model))
    coordinator = Coordinator(engine=engine, session=engine.session, stream_arn=model.Meta.stream["arn"])
    stream = Stream(model=model, engine=engine, coordinator=coordinator)
    return stream


class Stream:
    """Iterate over all records in a stream.

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

    .. warning::

        **Chronological order is not guaranteed for high throughput streams.**

        DynamoDB guarantees ordering:

        * within any single shard
        * across shards for a single hash/range key

        There is no way to exactly order records from adjacent shards.  High throughput streams
        provide approximate ordering using each record's "ApproximateCreationDateTime".

        Tables with a single partition guarantee order across all records.

        See :ref:`Stream Internals <internal-streams>` for details.
    """
    def __init__(self, *, model, engine, coordinator):

        self.model = model
        self.engine = engine
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

    def heartbeat(self):
        """Ensures iterators without fixed sequence numbers don't expire.

        You should call this every 12 minutes or more often.  This is an inexpensive operation.
        It averages 1 outbound call per 4 hours per shard, for a shard with any activity.

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
        """Move to either endpoint of the stream; a stream token; or a specific time.

        .. code-block:: python

            # Very fast
            stream.move_to("trim_horizon")

            # Fast
            stream.move_to(stream.token)

            # Very slow, scans from trim_horizon to the target time
            stream.move_to(arrow.now().replace(days=-1))

        """
        self.coordinator.move_to(position)

    @property
    def token(self):
        """Can be used to reconstruct the current progress of the iterator.

        .. code-block:: python

            same_stream = engine.stream(MyModel, position=stream.token)
            with open(".stream-state", "w") as f:
                json.dump(stream.token, f)

        :returns: Stream state as a json-friendly dict
        """
        return self.coordinator.token

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
