import logging
import warnings
from typing import Any, Callable, Union

from .conditions import render
from .exceptions import (
    InvalidModel,
    InvalidStream,
    InvalidTemplate,
    MissingObjects,
    UnknownType,
)
from .models import BaseModel, Index, subclassof, unpack_from_dynamodb
from .search import Search
from .session import SessionWrapper
from .signals import (
    before_create_table,
    model_bound,
    model_validated,
    object_deleted,
    object_loaded,
    object_saved,
)
from .stream import Stream
from .transactions import ReadTransaction, WriteTransaction
from .util import Sentinel, dump_key, extract_key, index_for, walk_subclasses


__all__ = ["Engine"]

logger = logging.getLogger("bloop.engine")
deprecated_false = Sentinel("Deprecated:False")

_sync_values = {
    "save": {
        None: "NONE",
        "new": "ALL_NEW",
        "old": "ALL_OLD"
    },
    "delete": {
        None: "NONE",
        "old": "ALL_OLD"
    }
}


def deprecate_atomic(x):
    if x is deprecated_false:
        return False
    warnings.warn(
        "The 'atomic=' kwarg will be removed in 3.0.0; see https://github.com/numberoverzero/bloop/issues/138",
        DeprecationWarning, stacklevel=3)
    return x


def validate_not_abstract(*objs):
    for obj in objs:
        if obj.Meta.abstract:
            cls = obj if isinstance(obj, type) else obj.__class__
            raise InvalidModel("{!r} is abstract.".format(cls.__name__))


def validate_is_model(model):
    if not subclassof(model, BaseModel):
        cls = model if isinstance(model, type) else model.__class__
        raise InvalidModel("{!r} does not subclass BaseModel.".format(cls.__name__))


def validate_sync(mode, value):
    allowed = _sync_values[mode]
    wire = allowed.get(value)
    if wire is None:
        raise ValueError(f"Unrecognized option {value!r} for sync parameter, must be one of {set(allowed.keys())}")
    return wire


def fail_unknown(model, ctx):
    # Best-effort check for a more helpful message
    msg = "{!r} does not support the Type interface."
    obj = getattr(model, "__name__", model)
    raise UnknownType(msg.format(obj)) from ctx


TableNameFormatter = Callable[[Any], str]


def create_get_table_name_func(table_name_template: Union[str, TableNameFormatter]) -> TableNameFormatter:
    if isinstance(table_name_template, str):
        if "{table_name}" not in table_name_template:
            raise InvalidTemplate("table name template must contain '{table_name}'")
        return lambda o: table_name_template.format(table_name=o.Meta.table_name)
    elif callable(table_name_template):
        return table_name_template
    else:
        raise ValueError("table name template must be a string or function")


class Engine:
    """Primary means of interacting with DynamoDB.

    To apply a prefix to each model's table name, you can use a simple format string:

    .. code-block:: pycon

        >>> template = "my-prefix-{table_name}"
        >>> engine = Engine(table_name_template=template)

    For more complex table_name customization, you can provide a function:

    .. code-block:: pycon

        >>> def reverse_name(model):
        ...     return model.Meta.table_name[::-1]
        >>> engine = Engine(table_name_template=reverse_name)

    :param dynamodb: DynamoDB client.  Defaults to ``boto3.client("dynamodb")``.
    :param dynamodbstreams: DynamoDBStreams client.  Defaults to ``boto3.client("dynamodbstreams")``.
    :param table_name_template: Customize the table name of each model bound to the engine.  If a string
        is provided, string.format(table_name=model.Meta.table_name) will be called.  If a function is provided, the
        function will be called with the model as its sole argument.  Defaults to "{table_name}".
    """
    def __init__(
            self, *,
            dynamodb=None, dynamodbstreams=None,
            table_name_template: Union[str, TableNameFormatter] = "{table_name}"):
        self._compute_table_name = create_get_table_name_func(table_name_template)
        self.session = SessionWrapper(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)

    def _dump(self, model, obj, context=None, **kwargs):
        context = context or {"engine": self}
        try:
            dump = model._dump
        except AttributeError as e:
            fail_unknown(model, e)
        else:
            return dump(obj, context=context, **kwargs)

    def _load(self, model, value, context=None, **kwargs):
        context = context or {"engine": self}
        try:
            load = model._load
        except AttributeError as e:
            fail_unknown(model, e)
        else:
            return load(value, context=context, **kwargs)

    def bind(self, model, *, skip_table_setup=False):
        """Create backing tables for a model and its non-abstract subclasses.

        :param model: Base model to bind.  Can be abstract.
        :param skip_table_setup: Don't create or verify the table in DynamoDB.  Default is False.
        :raises bloop.exceptions.InvalidModel: if ``model`` is not a subclass of :class:`~bloop.models.BaseModel`.
        """
        # Make sure we're looking at models
        validate_is_model(model)

        concrete = set(filter(lambda m: not m.Meta.abstract, walk_subclasses(model)))
        if not model.Meta.abstract:
            concrete.add(model)
        logger.debug("binding non-abstract models {}".format(
            sorted(c.__name__ for c in concrete)
        ))

        # create_table doesn't block until ACTIVE or validate.
        # It also doesn't throw when the table already exists, making it safe
        # to call multiple times for the same unbound model.
        if skip_table_setup:
            logger.info("skip_table_setup is True; not trying to create tables or validate models during bind")
        else:
            self.session.clear_cache()

        is_creating = {}

        for model in concrete:
            table_name = self._compute_table_name(model)
            before_create_table.send(self, engine=self, model=model)
            if not skip_table_setup:
                if table_name in is_creating:
                    continue
                creating = self.session.create_table(table_name, model)
                is_creating[table_name] = creating

        for model in concrete:
            if not skip_table_setup:
                table_name = self._compute_table_name(model)
                if is_creating[table_name]:
                    # polls until table is active
                    self.session.describe_table(table_name)
                    if model.Meta.ttl:
                        self.session.enable_ttl(table_name, model)
                    if model.Meta.backups and model.Meta.backups["enabled"]:
                        self.session.enable_backups(table_name, model)
                self.session.validate_table(table_name, model)
                model_validated.send(self, engine=self, model=model)
            model_bound.send(self, engine=self, model=model)

        logger.info("successfully bound {} models to the engine".format(len(concrete)))

    def delete(self, *objs, condition=None, atomic=deprecated_false, sync=None):
        """Delete one or more objects.

        :param objs: objects to delete.
        :param condition: only perform each delete if this condition holds.
        :param bool atomic: only perform each delete if the local and DynamoDB versions of the object match.
            **This parameter is deprecated and will be removed in 3.0**
        :param sync:
            update objects after deleting.  "old" loads attributes before the delete;
            None does not mutate the object locally.  Default is None.
        :raises bloop.exceptions.ConstraintViolation: if the condition (or atomic) is not met.
        """
        objs = set(objs)
        validate_not_abstract(*objs)
        validate_sync("delete", sync)
        for obj in objs:
            attrs = self.session.delete_item({
                "TableName": self._compute_table_name(obj.__class__),
                "Key": dump_key(self, obj),
                "ReturnValues": validate_sync("delete", sync),
                **render(self, obj=obj, atomic=deprecate_atomic(atomic), condition=condition)
            })
            if attrs is not None:
                unpack_from_dynamodb(attrs=attrs, expected=obj.Meta.columns, engine=self, obj=obj)
            object_deleted.send(self, engine=self, obj=obj)
        logger.info("successfully deleted {} objects".format(len(objs)))

    def load(self, *objs, consistent=False):
        """Populate objects from DynamoDB.

        :param objs: objects to delete.
        :param bool consistent: Use `strongly consistent reads`__ if True.  Default is False.
        :raises bloop.exceptions.MissingKey: if any object doesn't provide a value for a key column.
        :raises bloop.exceptions.MissingObjects: if one or more objects aren't loaded.

        __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
        """
        get_table_name = self._compute_table_name
        objs = set(objs)
        validate_not_abstract(*objs)

        table_index, object_index, request = {}, {}, {}

        for obj in objs:
            table_name = get_table_name(obj.__class__)
            key = dump_key(self, obj)
            index = index_for(key)

            if table_name not in object_index:
                table_index[table_name] = list(sorted(key.keys()))
                object_index[table_name] = {}
                request[table_name] = {"Keys": [], "ConsistentRead": consistent}

            if index not in object_index[table_name]:
                request[table_name]["Keys"].append(key)
                object_index[table_name][index] = set()
            object_index[table_name][index].add(obj)

        response = self.session.load_items(request)

        for table_name, list_of_attrs in response.items():
            for attrs in list_of_attrs:
                key_shape = table_index[table_name]
                key = extract_key(key_shape, attrs)
                index = index_for(key)

                for obj in object_index[table_name].pop(index):
                    unpack_from_dynamodb(
                        attrs=attrs, expected=obj.Meta.columns, engine=self, obj=obj)
                    object_loaded.send(self, engine=self, obj=obj)
                if not object_index[table_name]:
                    object_index.pop(table_name)

        if object_index:
            not_loaded = set()
            for index in object_index.values():
                for index_set in index.values():
                    not_loaded.update(index_set)
            logger.info("loaded {} of {} objects".format(len(objs) - len(not_loaded), len(objs)))
            raise MissingObjects("Failed to load some objects.", objects=not_loaded)
        logger.info("successfully loaded {} objects".format(len(objs)))

    def query(self, model_or_index, key, filter=None, projection="all", consistent=False, forward=True):
        """Create a reusable :class:`~bloop.search.QueryIterator`.

        :param model_or_index: A model or index to query.  For example, ``User`` or ``User.by_email``.
        :param key:
            Key condition.  This must include an equality against the hash key, and optionally one
            of a restricted set of conditions on the range key.
        :param filter: Filter condition.  Only matching objects will be included in the results.
        :param projection:
            "all", "count", a list of column names, or a list of :class:`~bloop.models.Column`.  When projection is
            "count", you must advance the iterator to retrieve the count.
        :param bool consistent: Use `strongly consistent reads`__ if True.  Default is False.
        :param bool forward:  Query in ascending or descending order.  Default is True (ascending).

        :return: A reusable query iterator with helper methods.
        :rtype: :class:`~bloop.search.QueryIterator`

        __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
        """
        if isinstance(model_or_index, Index):
            model, index = model_or_index.model, model_or_index
        else:
            model, index = model_or_index, None
        validate_not_abstract(model)
        q = Search(
            mode="query", engine=self, model=model, index=index, key=key, filter=filter,
            projection=projection, consistent=consistent, forward=forward)
        return iter(q.prepare())

    def save(self, *objs, condition=None, atomic=deprecated_false, sync=None):
        """Save one or more objects.

        :param objs: objects to save.
        :param condition: only perform each save if this condition holds.
        :param bool atomic: only perform each save if the local and DynamoDB versions of the object match.
            **This parameter is deprecated and will be removed in 3.0**
        :param sync:
            update objects after saving.  "new" loads attributes after the save;
            "old" loads attributes before the save; None does not mutate the object locally.  Default is None.
        :raises bloop.exceptions.ConstraintViolation: if the condition (or atomic) is not met.
        """
        objs = set(objs)
        validate_not_abstract(*objs)
        for obj in objs:
            attrs = self.session.save_item({
                "TableName": self._compute_table_name(obj.__class__),
                "Key": dump_key(self, obj),
                "ReturnValues": validate_sync("save", sync),
                **render(self, obj=obj, atomic=deprecate_atomic(atomic), condition=condition, update=True)
            })
            if attrs is not None:
                unpack_from_dynamodb(attrs=attrs, expected=obj.Meta.columns, engine=self, obj=obj)
            object_saved.send(self, engine=self, obj=obj)
        logger.info("successfully saved {} objects".format(len(objs)))

    def scan(self, model_or_index, filter=None, projection="all", consistent=False, parallel=None):
        """Create a reusable :class:`~bloop.search.ScanIterator`.

        :param model_or_index: A model or index to scan.  For example, ``User`` or ``User.by_email``.
        :param filter: Filter condition.  Only matching objects will be included in the results.
        :param projection:
            "all", "count", a list of column names, or a list of :class:`~bloop.models.Column`.  When projection is
            "count", you must exhaust the iterator to retrieve the count.
        :param bool consistent: Use `strongly consistent reads`__ if True.  Default is False.
        :param tuple parallel: Perform a `parallel scan`__.  A tuple of (Segment, TotalSegments)
            for this portion the scan. Default is None.
        :return: A reusable scan iterator with helper methods.
        :rtype: :class:`~bloop.search.ScanIterator`

        __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
        __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#QueryAndScanParallelScan
        """
        if isinstance(model_or_index, Index):
            model, index = model_or_index.model, model_or_index
        else:
            model, index = model_or_index, None
        validate_not_abstract(model)
        s = Search(
            mode="scan", engine=self, model=model, index=index, filter=filter,
            projection=projection, consistent=consistent, parallel=parallel)
        return iter(s.prepare())

    def stream(self, model, position):
        """Create a :class:`~bloop.stream.Stream` that provides approximate chronological ordering.

        .. code-block:: pycon

            # Create a user so we have a record
            >>> engine = Engine()
            >>> user = User(id=3, email="user@domain.com")
            >>> engine.save(user)
            >>> user.email = "admin@domain.com"
            >>> engine.save(user)

            # First record lacks an "old" value since it's an insert
            >>> stream = engine.stream(User, "trim_horizon")
            >>> next(stream)
            {'key': None,
             'old': None,
             'new': User(email='user@domain.com', id=3, verified=None),
             'meta': {
                 'created_at': datetime.datetime(2016, 10, 23, ...),
                 'event': {
                     'id': '3fe6d339b7cb19a1474b3d853972c12a',
                     'type': 'insert',
                     'version': '1.1'},
                 'sequence_number': '700000000007366876916'}
            }


        :param model: The model to stream records from.
        :param position: "trim_horizon", "latest", a stream token, or a :class:`datetime.datetime`.
        :return: An iterator for records in all shards.
        :rtype: :class:`~bloop.stream.Stream`
        :raises bloop.exceptions.InvalidStream: if the model does not have a stream.
        """
        validate_not_abstract(model)
        if not model.Meta.stream or not model.Meta.stream.get("arn"):
            raise InvalidStream("{!r} does not have a stream arn".format(model))
        stream = Stream(model=model, engine=self)
        stream.move_to(position=position)
        return stream

    def transaction(self, mode="w"):
        """
        Create a new :class:`~bloop.transactions.ReadTransaction` or :class:`~bloop.transactions.WriteTransaction`.

        As a context manager, calling commit when the block exits:

        .. code-block:: pycon

            >>> engine = Engine()
            >>> user = User(id=3, email="user@domain.com")
            >>> tweet = Tweet(id=42, data="hello, world")
            >>> with engine.transaction("w") as tx:
            ...     tx.delete(user)
            ...     tx.save(tweet, condition=Tweet.id.is_(None))

        Or manually calling prepare and commit:

        .. code-block:: pycon

            >>> engine = Engine()
            >>> user = User(id=3, email="user@domain.com")
            >>> tweet = Tweet(id=42, data="hello, world")
            >>> tx = engine.transaction("w")
            >>> tx.delete(user)
            >>> tx.save(tweet, condition=Tweet.id.is_(None))
            >>> tx.prepare().commit()

        :param str mode: Either "r" or "w" to create a ReadTransaction or WriteTransaction.  Default is "w"
        :return: A new transaction that can be committed.
        :rtype: :class:`~bloop.transactions.ReadTransaction` or :class:`~bloop.transactions.WriteTransaction`
        """
        if mode == "r":
            cls = ReadTransaction
        elif mode == "w":
            cls = WriteTransaction
        else:
            raise ValueError(f"unknown mode {mode}")
        return cls(self)
