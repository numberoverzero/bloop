import bloop.column
import bloop.condition
import bloop.index
import bloop.tracking
import bloop.util
import operator

SELECT_MODES = {
    "all": "ALL_ATTRIBUTES",
    "projected": "ALL_PROJECTED_ATTRIBUTES",
    "specific": "SPECIFIC_ATTRIBUTES",
    "count": "COUNT"
}


def _consume(iter):
    for _ in iter:
        pass


def _validate_hash_key_condition(condition):
    # 1 Must be comparison
    if (isinstance(condition, bloop.condition.Comparison) and
            # 2 Must be EQ compariator
            (condition.comparator is operator.eq) and
            # 3 Must not have a path component
            (not condition.path)):
        return True
    raise ValueError("KeyCondition must be EQ, without any document paths")


def _validate_range_key_condition(condition):
    if isinstance(condition, (bloop.condition.BeginsWith,
                              bloop.condition.Between)):
        return True
    elif isinstance(condition, bloop.condition.Comparison):
        # Valid comparators are EG | LE | LT | GE | GT -- not NE
        if condition.comparator is not operator.ne:
            return True
    raise ValueError("Invalid KeyCondition {}".format(condition))


def _validate_key_condition(key_condition, hash_column, range_column):
    # 0. Must specify at least a hash condition
    if not key_condition:
        raise ValueError("At least one key condition (hash) is required")

    # 1. Comparison condition, single column
    if isinstance(key_condition, bloop.condition.Comparison):
        # 1.1 Comparison, EQ, no path
        _validate_hash_key_condition(key_condition)
        # 1.2 Must be a condition on hash_column
        if key_condition.column is not hash_column:
            raise ValueError("KeyCondition must compare against hash column")
    # 2. AND is valid for hash, range combinations
    elif isinstance(key_condition, bloop.condition.And):
        key_columns = {hash_column}
        if range_column is not None:
            key_columns.add(range_column)

        # 2.1 `and` can specify at most as many conditions as there are
        #     key columns (1 or 2)
        if len(key_condition.conditions) > len(key_columns):
            msg = "Only {} key conditions allowed but {} provided".format(
                len(key_columns), len(key_condition))
            raise ValueError(msg)

        has_hash_condition = False
        for subcondition in key_condition.conditions:
            if not hasattr(subcondition, "column"):
                raise ValueError(
                    "Condition can't be made up of And/Or/Not conditions")
            if subcondition.column is hash_column:
                # 2.2 No more than one condition against the hash column
                if has_hash_condition:
                    raise ValueError(
                        "Must specify a condition on the hash key")
                _validate_hash_key_condition(subcondition)
                has_hash_condition = True
            elif subcondition.column is range_column:
                # 2.3 Range conditions can be <,<=,>,>=,==, Between, BeginsWith
                _validate_range_key_condition(subcondition)
            else:
                # 2.4 Conditions must be against hash or range columns
                msg = "Non-key condition {} passed as KeyCondition"
                raise ValueError(msg.format(subcondition))

        # 2.4 At least one condition against the hash column
        if not has_hash_condition:
            raise ValueError("Must specify a condition on the hash key")
    # 3. Must be EQ or AND
    else:
        raise ValueError("KeyCondition must be EQ or AND")


def _validate_prefetch(value):
    invalid = ValueError("prefetch must be 'all' or a non-negative int")
    if value != "all":
        try:
            value = int(value)
            if value < 0:
                raise invalid
        except ValueError:
            raise invalid
    return value


def _validate_select_mode(select):
    invalid = ValueError("Must specify 'all', 'projected', or"
                         " a list of column objects to select")
    if not select:
            raise invalid
    if isinstance(select, str):
        select = select.lower()
        if select not in ["all", "projected"]:
            raise invalid
    else:
        select = list(select)
        if not bloop.util.areinstance(select, bloop.column.Column):
            raise invalid
    return select


def _is_select_exact(index, engine):
    """
    Returns True if :
    1) The filter is on a GSI, or an LSI and the engine is strict
    2) The index projection is not ALL
    """
    is_gsi = isinstance(index, bloop.index.GlobalSecondaryIndex)
    is_lsi = isinstance(index, bloop.index.LocalSecondaryIndex)
    strict = engine.config["strict"]
    requires_exact = is_gsi or (is_lsi and strict)
    is_exact = (not requires_exact) or (index.projection == "ALL")
    return is_exact


class _Filter(object):
    """Base class for Scan and Query."""
    # Scan -> "scan", Query -> "query"
    filter_type = None

    def __init__(self, engine, *, model=None, index=None):
        self.engine = engine
        self.model = model
        self.index = index

        self._key_condition = bloop.condition.Condition()
        self._filter_condition = bloop.condition.Condition()
        if self.index:
            self._select = "projected"
        else:
            self._select = "all"
        self._forward = True
        self._consistent = engine.config["consistent"]

        self._select_columns = []

    def _copy(self):
        cls = self.__class__
        other = cls(engine=self.engine, model=self.model, index=self.index)

        for attr in ["_filter_condition", "_key_condition",
                     "_select", "_forward", "_consistent"]:
            setattr(other, attr, getattr(self, attr))

        other._select_columns = list(self._select_columns)
        other._key_condition = self._key_condition
        return other

    def _expected(self):
        """
        Return a list of Columns that are expected for the current options.
        """
        if self._select == "all":
            return self.model.Meta.columns
        elif self._select == "projected":
            return self.index.projection_attributes
        # specific
        else:
            # If more are requested than a LSI supports, all will be loaded.
            # In all other cases, just the selected columns will be.
            if isinstance(self.index, bloop.index.LocalSecondaryIndex):
                selected = set(self._select_columns)
                available = self.index.projection_attributes
                if not selected.issubset(available):
                    return self.model.Meta.columns
            return self._select_columns

    def _generate_request(self, renderer):
        request = {
            "TableName": self.model.Meta.table_name,
            "Select": SELECT_MODES[self._select],
            "ConsistentRead": self._consistent
        }
        if self.index:
            request["IndexName"] = self.index.dynamo_name
        if self._filter_condition:
            renderer.render(self._filter_condition, mode="filter")
        if self._select == "specific":
            renderer.projection(self._select_columns)
        request.update(renderer.rendered)
        return request

    def all(self, prefetch=None):
        """Creates the FilterResult that will lazy load the results of the
        scan/query.

        Usage:
            Building a query iteratively::

                base_query = engine.query(Model).key(id="foo")
                query = base_query.consistent.ascending

                # Iterate results directly, discarding query metadata
                for result in query:
                    ...

                # Save reference to FilterResult instance
                results = query.all()
                for result in results:
                    ...
                results.count
                results.scanned_count

        """
        if prefetch is None:
            prefetch = self.engine.config["prefetch"]
        # dynamo.client.query or dynamo.client.scan
        call = getattr(self.engine.client, self.filter_type)
        renderer = bloop.condition.ConditionRenderer(self.engine)
        request = self._generate_request(renderer)

        expected = self._expected()
        return FilterResult(prefetch, call, request, self.engine,
                            self.model, expected)

    @property
    def ascending(self):
        other = self._copy()
        other._forward = True
        return other

    @property
    def consistent(self):
        if isinstance(self.index, bloop.index.GlobalSecondaryIndex):
            raise ValueError(
                "Cannot use ConsistentRead with a GlobalSecondaryIndex")
        other = self._copy()
        other._consistent = True
        return other

    def count(self):
        other = self._copy()
        other._select = "count"
        other._select_columns.clear()
        # Force fetch all
        result = other.all(prefetch="all")
        return {
            "count": result.count,
            "scanned_count": result.scanned_count
        }

    @property
    def descending(self):
        other = self._copy()
        other._forward = False
        return other

    def filter(self, condition):
        other = self._copy()
        other._filter_condition = condition
        return other

    def first(self):
        """ Returns the first result that matches the filter. """
        result = self.all(prefetch=0)
        return result.first

    def key(self, condition):
        obj = self.index or self.model.Meta
        hash_column = obj.hash_key
        range_column = obj.range_key

        _validate_key_condition(condition, hash_column, range_column)

        other = self._copy()
        other._key_condition = condition
        return other

    def select(self, columns):
        """
        columns must be "all", "projected", or a list of `bloop.Column` objects
        """
        select = _validate_select_mode(columns)
        # False for non-index queries.
        # True if we need to query exactly, but the index's projection
        # doesn't support fetching all attributes.  Invalid to select all,
        # possibly valid to select specific.
        is_exact = _is_select_exact(self.index, self.engine)

        if select == "projected":
            if not self.index:
                raise ValueError("Cannot select 'projected' attributes"
                                 " without an index")
            other = self._copy()
            other._select = select
            other._select_columns.clear()
            return other

        elif select == "all":
            if not is_exact:
                raise ValueError("Cannot select 'all' attributes from a GSI"
                                 " (or an LSI in strict mode) unless the"
                                 " index's projection is 'ALL'")
            other = self._copy()
            other._select = select
            other._select_columns.clear()
            return other

        # select is a list of model names, use "specific"
        else:
            if not is_exact:
                missing_attrs = set(select) - self.index.projection_attributes
                if missing_attrs:
                    msg = ("Index projection is missing the following expected"
                           " attributes, and is either a GSI or an LSI and"
                           " strict mode is enabled: {}").format(missing_attrs)
                    raise ValueError(msg)
            other = self._copy()
            other._select = "specific"
            other._select_columns = select
            return other

    def __iter__(self):
        return iter(self.all())


class Query(_Filter):
    filter_type = "query"

    def _generate_request(self, renderer):
        request = super()._generate_request(renderer)
        request["ScanIndexForward"] = self._forward

        if not self._key_condition:
            raise ValueError("Must specify at least a hash key condition")
        renderer.render(self._key_condition, mode="key")
        request.update(renderer.rendered)
        return request


class Scan(_Filter):
    filter_type = "scan"


class FilterResult(object):
    """
    Result from a scan or query.  Usually lazy loaded, iterate to execute.

    Uses engine.prefetch to control call batching
    """
    def __init__(self, prefetch, call, request, engine, model, expected):
        self._call = call
        self._prefetch = _validate_prefetch(prefetch)
        self.request = request
        self.engine = engine
        self.model = model
        self.expected = expected

        self.count = 0
        self.scanned_count = 0
        self._results = []
        self._continue = None
        self._complete = False

        # Kick off the full execution
        if prefetch == "all":
            _consume(self)

    @property
    def complete(self):
        return self._complete

    @property
    def first(self):
        # Advance until we have some results, or we exhaust the query
        step = iter(self)
        while not self._results and not self.complete:
            try:
                next(step)
            except StopIteration:
                # The step above exhausted the results, nothing left
                break

        if not self._results:
            raise ValueError("No results found.")
        return self._results[0]

    @property
    def results(self):
        if not self.complete:
            raise RuntimeError("Can't access results until request exhausted")
        return self._results

    def __iter__(self):
        # Already finished, iterate existing list
        if self.complete:
            return iter(self.results)
        # Fully exhaust the filter before returning an iterator
        elif self._prefetch == "all":
            # Give self._continue a chance to be not None
            _consume(self._step())
            while self._continue:
                _consume(self._step())
            self._complete = True
            return iter(self.results)
        # Lazy load, prefetching as necessary
        else:
            return self._prefetch_iter()

    def _prefetch_iter(self):
        """
        Separate function because the `yield` statement would turn __iter__
        into a generator when we want to return existing iterators in some
        cases.
        """
        while not self.complete:
            prefetch = self._prefetch

            objs = list(self._step())
            while self._continue and prefetch:
                prefetch -= 1
                # Doesn't need the same catch on StopIteration as in `first`
                # since self._continue would be set on the above _step call
                objs.extend(self._step())
            for obj in objs:
                    yield obj

            # Don't set complete until we've
            # yielded all objects from this step
            if not self._continue:
                self._complete = True

    def _step(self):
        """ Single call, advancing ExclusiveStartKey if necessary. """
        if self._continue:
            self.request["ExclusiveStartKey"] = self._continue
        response = self._call(self.request)
        self._continue = response.get("LastEvaluatedKey", None)

        self.count += response["Count"]
        self.scanned_count += response["ScannedCount"]

        results = response.get("Items", [])
        for result in results:
            obj = self.engine._instance(self.model)
            self.engine._update(obj, result, self.expected)
            bloop.tracking.sync(obj, self.engine)

            self._results.append(obj)
            yield obj
