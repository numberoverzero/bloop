from ..signals import object_loaded
from ..util import unpack_from_dynamodb
from .coordinator import Coordinator


class Stream:
    """Iterator over all records in a stream.

    :param model: The model to stream records from.
    :param engine: The engine to load model objects through.
    """
    def __init__(self, *, model, engine):

        self.model = model
        self.engine = engine
        self.coordinator = Coordinator(
            engine=engine,
            session=engine.session,
            stream_arn=model.Meta.stream["arn"])

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
        """Refresh iterators without sequence numbers so they don't expire.

        Call this at least every 14 minutes.
        """
        self.coordinator.heartbeat()

    def move_to(self, position):
        """

        * Move to either endpoint of the stream with "trim_horizon" or "latest".
        * Move to a stream token (``other_stream.token``)
        * Move to a specific time ie. ``arrow.now().replace(hours=-2)``
        """
        self.coordinator.move_to(position)

    @property
    def token(self):
        """JSON-serializable representation of the current state.

        Use :func:`Engine.stream(YourModel, token) <bloop.engine.Engine.stream>` to create an identical stream,
        or :func:`stream.move_to(token) <bloop.stream.Stream.move_to>` to move an existing stream to this position.

        :returns: Stream state as a json-friendly dict
        :rtype: dict
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
