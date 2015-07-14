import bloop.column
import bloop.condition
import bloop.index
import operator

missing = object()
SELECT_MODES = {
    "all": "ALL_ATTRIBUTES",
    "projected": "ALL_PROJECTED_ATTRIBUTES",
    "specific": "SPECIFIC_ATTRIBUTES",
    "count": "COUNT"
}


def consume(iter):
    for _ in iter:
        pass


def validate_key_condition(condition):
    if isinstance(condition, (bloop.condition.BeginsWith,
                              bloop.condition.Between)):
        return True
    elif isinstance(condition, bloop.condition.Comparison):
        # Valid comparators are EG | LE | LT | GE | GT -- not NE
        if condition.comparator is not operator.ne:
            return True
    raise ValueError("Invalid KeyCondition {}".format(condition))


def validate_select_mode(select):
    invalid = ValueError("Must specify 'all', 'projected', 'count', or"
                         " a list of column objects to select")
    if isinstance(select, str):
        select = select.lower()
        if select not in ["all", "projected", "count"]:
            raise invalid
    else:
        try:
            select = set(select)
        except TypeError:
            raise invalid
        if not select:
            raise invalid
        for column in select:
            if not bloop.column.is_column(column):
                raise invalid
    return select


class Filter(object):
    '''
    Base class for Scan and Query.

    The functions key, filter, select, ascending, descending,
    and consistent all return copies of the Filter object with the
    expected modifications.

    Example:
        f = Filter(engine, model)
        f2 = f.descending
        f3 = f.consistent

        assert not f._consistent
        assert not f2._consistent
        assert f3._consistent

        assert f._forward
        assert not f2._forward
        assert f3._forward

    '''
    # Scan -> 'scan, Query -> 'query'
    filter_type = "filter"

    def __init__(self, engine, *, model=None, index=None):
        self.engine = engine
        self.model = model
        self.index = index

        self._key_condition = None
        self._filter_condition = None
        if self.index:
            self._select = "projected"
        else:
            self._select = "all"
        self._forward = True
        self._consistent = False

        self._select_columns = set()

    def copy(self):
        cls = self.__class__
        other = cls(engine=self.engine, model=self.model, index=self.index)

        for attr in ["_filter_condition", "_key_condition",
                     "_select", "_forward", "_consistent"]:
            setattr(other, attr, getattr(self, attr))

        other._select_columns = set(self._select_columns)
        other._key_condition = self._key_condition

        return other

    def key(self, condition):
        # AND multiple conditions
        if self._key_condition:
            condition &= self._key_condition

        obj = self.index or self.model.Meta
        hash_column = obj.hash_key
        range_column = obj.range_key

        max_conditions = 1 + bool(range_column)

        if not condition:
            raise ValueError("At least one key condition (hash) is required")

        # AND is allowed so long as the index we're using allows hash + range
        if isinstance(condition, bloop.condition.And):
            if max_conditions < len(condition):
                msg = ("Model or Index only allows {} condition(s) but"
                       " an AND of {} condition(s) was supplied.").format(
                           max_conditions, len(condition))
                raise ValueError(msg)
            # KeyConditions can only use the following:
            # EQ | LE | LT | GE | GT | BEGINS_WITH | BETWEEN
            for subcond in condition.conditions:
                validate_key_condition(subcond)

            columns = set(subcond.column for subcond in condition.conditions)
            # Duplicate column in AND
            if len(columns) < len(condition):
                raise ValueError("Cannot use a hash/range column more"
                                 " than once when specifying KeyConditions")

            if hash_column not in columns:
                raise ValueError("Must specify a hash key")

            # At this point we've got the same number of columns and
            # conditions, and that's less than or equal to the number of
            # allowed conditions for this model/index.

        # Simply validate all other conditions
        else:
            validate_key_condition(condition)
            if condition.column is not hash_column:
                raise ValueError("Must specify a hash key")

        other = self.copy()
        other._key_condition = condition
        return other

    def filter(self, condition):
        other = self.copy()
        # AND multiple filters
        if other._filter_condition:
            condition &= other._filter_condition
        other._filter_condition = condition
        return other

    def select(self, columns):
        '''
        columns must be 'all', 'projected', or a list of `bloop.Column` objects
        '''

        # Rules for select are a bit convoluted, and easy to get wrong.
        # While it's easy to want to guess at intent -- such as
        # transforming 'all' with a GSI into 'projected' -- that can
        # easily result in cases where more or less attributes are returned
        # than were expected.  Instead, we'll carefully validate the index
        # projection, the select mode, and the list of attributes requested.

        # Combinations:
        # key: (select mode, index, index projection)
        # ('projected', [LSI, GSI]) -> Valid
        # ('projected', no index) -> Invalid, must specify an index

        # ('all', no index) -> Valid
        # ('all', LSI, ['all', 'key', 'select']) -> Valid*
        # ('all', GSI, 'all') -> Valid
        # ('all', GSI, 'key') -> Invalid: key-only GSI can't load all attrs
        # ('all', GSI, 'select') -> Invalid: even if GSI projection contains
        #                                    all attributes of the table

        # ('specific', no index) -> Valid
        # ('specific', LSI, ['all', 'key', 'select']) -> Valid*
        # ('specific', GSI, 'all') -> Valid
        # ('specific', GSI, 'key') -> IFF all attrs in GSI's keys
        # ('specific', GSI, 'select') -> IFF all attrs in GSI's projected attrs

        # *: Any queries against an LSI whose set of requested attributes are
        #    a superset of the LSI's projected attributes will incure extra
        #    reads against the table.  While these are valid, it may be worth
        #    introducing a config variable to strictly disallow extra reads.
        select = validate_select_mode(columns)

        is_gsi = bloop.index.is_global_index(self.index)

        if select == "count":
            other = self.copy()
            other._select = select
            other._select_columns.clear()
            return other

        elif select == 'projected':
            if not self.index:
                raise ValueError("Cannot select 'projected' attributes"
                                 " without an index")
            other = self.copy()
            other._select = select
            other._select_columns.clear()
            return other

        elif select == 'all':
            if is_gsi and self.index.projection != "ALL":
                raise ValueError("Cannot select 'all' attributes from a GSI"
                                 " unless the GSI's projection is 'ALL'")
            other = self.copy()
            other._select = select
            other._select_columns.clear()
            return other

        # select is a list of model names, use 'specific'
        else:
            # Combine before validation, since the total set may no longer
            # be valid for the index.
            other = self.copy()
            other._select = 'specific'
            other._select_columns.update(select)

            if is_gsi and not self.index.projection == "ALL":
                projected = self.index.projection_attributes
                missing_attrs = other._select_columns - projected
                if missing_attrs:
                    msg = "Projection is missing the following attributes: {}"
                    msg_attrs = [attr.model_name for attr in missing_attrs]
                    raise ValueError(msg.format(msg_attrs))
            return other

    def count(self):
        other = self.copy()
        other._select = "count"
        other._select_columns.clear()
        # Force fetch all
        result = other.all(prefetch=-1)
        return {
            "count": result.count,
            "scanned_count": result.scanned_count
        }

    def all(self, prefetch=None):
        '''
        Unless prefetch is < 0, simply creates the FilterResult that will
        lazy load the results of the scan/query.  Unlike `iter(self)` this
        returns the FilterResult object, which allows inspection of the
        `count` and `scanned_count` attributes.  Iterating over the result
        object will not trigger a new scan/query, while iterating over a
        scan/query will ALWAYS result in a new scan/query being executed.

        Usage:

        base_query = engine.query(Model).key(id='foo')
        query = base_query.consistent.ascending

        # Iterate results directly, discarding query metadata
        for result in query:
            ...

        # Save reference to FilterResult instance
        results = query.all()
        for result in results:
            ...
        print(results.count, results.scanned_count)
        '''
        if prefetch is None:
            prefetch = self.engine.prefetch[self.filter_type]
        # dynamo.client.query or dynamo.client.scan
        call = getattr(self.engine.client, self.filter_type)
        renderer = bloop.condition.ConditionRenderer(self.engine)
        request = self.generate_request(renderer)
        return FilterResult(prefetch, call, request, self.engine, self.model)

    def first(self):
        '''
        Returns the first result that matches the filter.

        Forces prefetch=0 for the fastest return - continuation tokens will
        only be followed until a page with at least one result is returned.

        Faster than `Filter.all().first` unless:
        - prefetch = 0
        - prefetch > 0 AND first result is on page x, where x % (prefetch) == 0
        If either is true, Filter.all().first will have comparable performance.
        '''
        result = self.all(prefetch=0)
        return result.first

    def __iter__(self):
        return iter(self.all())

    @property
    def ascending(self):
        other = self.copy()
        other._forward = True
        return other

    @property
    def descending(self):
        other = self.copy()
        other._forward = False
        return other

    @property
    def consistent(self):
        other = self.copy()
        other._consistent = True
        return other

    def generate_request(self, renderer):
        request = {
            'TableName': self.model.Meta.table_name,
            'Select': SELECT_MODES[self._select]
        }
        if self.index:
            request['IndexName'] = self.index.dynamo_name
        if self._filter_condition:
            request.update(renderer.render(self._filter_condition,
                                           mode="filter"))
        if self._select == "specific":
            if not self._select_columns:
                raise ValueError(
                    "Must provide columns to get with 'specific' mode")
            names = map(renderer.name_ref, self._select_columns)
            request['ProjectionExpression'] = ", ".join(names)
        return request


class Query(Filter):
    filter_type = "query"

    def generate_request(self, renderer):
        request = super().generate_request(renderer)
        request['ScanIndexForward'] = self._forward
        request['ConsistentRead'] = self._consistent

        if not self._key_condition:
            raise ValueError("Must specify at least a hash key condition")

        if bloop.index.is_global_index(self.index) and self._consistent:
            raise ValueError(
                "Cannot use ConsistentRead with a GlobalSecondaryIndex")

        request.update(renderer.render(self._key_condition, mode="key"))

        return request


class Scan(Filter):
    filter_type = "scan"


class FilterResult(object):
    '''
    Result from a scan or query.  Usually lazy loaded, iterate to execute.

    Uses engine.prefetch to control call batching
    '''
    def __init__(self, prefetch, call, request, engine, model):
        self._call = call
        self._prefetch = prefetch
        self.request = request
        self.engine = engine
        self.model = model

        self.count = 0
        self.scanned_count = 0
        self._results = []

        self._continue = None
        self._complete = False

        # Kick off the full execution
        if prefetch < 0:
            consume(self)

    @property
    def results(self):
        if not self.complete:
            raise RuntimeError("Can't access results until request exhausted")
        return self._results

    @property
    def complete(self):
        return self._complete

    @property
    def first(self):
        if self._results:
            return self._results[0]

        if not self.complete:
            step = iter(self)
            # Advance until we have some results, or we exhaust the query
            while not self._results and not self.complete:
                next(step)

        # Either:
        # - filter was already complete
        # - filter is complete after above stepping
        # - filter is incomplete but there are some results
        if not self._results:
            raise ValueError("No results found.")
        return self._results[0]

    def __iter__(self):
        # Already finished, iterate existing list
        if self.complete:
            return iter(self.results)
        # Fully exhaust the filter before returning an iterator
        elif self._prefetch < 0:
            # Give self._continue a chance to be not None
            consume(self._step())
            while self._continue:
                consume(self._step())
            self._complete = True
            return iter(self.results)
        # Lazy load, prefetching as necessary
        else:
            return self.__prefetch_iter__()

    def __prefetch_iter__(self):
        '''
        Separate function because the `yield` statement would turn __iter__
        into a generator when we want to return existing iterators in some
        cases.
        '''
        while not self.complete:
            prefetch = self._prefetch

            objs = list(self._step())
            while self._continue and prefetch:
                prefetch -= 1
                objs.extend(self._step())
            for obj in objs:
                    yield obj

            # Don't set complete until we've
            # yielded all objects from this step
            if not self._continue:
                self._complete = True

    def _step(self):
        ''' Single call, advancing ExclusiveStartKey if necessary. '''
        if self._continue:
            self.request["ExclusiveStartKey"] = self._continue
        response = self._call(**self.request)
        self._continue = response.get("LastEvaluatedKey", None)

        self.count += response["Count"]
        self.scanned_count += response["ScannedCount"]

        results = response.get("Items", [])
        for result in results:
            obj = self.engine.__load__(self.model, result)
            self._results.append(obj)
            yield obj
