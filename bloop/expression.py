# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax
import bloop.column
import bloop.condition
import operator

missing = object()
SELECT_MODES = {
    "all": "ALL_ATTRIBUTES",
    "projected": "ALL_PROJECTED_ATTRIBUTES",
    "count": "COUNT",
    "specific": "SPECIFIC_ATTRIBUTES"
}


def consume(iter):
    for _ in iter:
        pass


def validate_key_condition(condition):
    if isinstance(condition, bloop.condition.BeginsWith):
        return True
    elif isinstance(condition, bloop.condition.Between):
        return True
    elif isinstance(condition, bloop.condition.Comparison):
        # Valid comparators are EG | LE | LT | GE | GT -- not NE
        return condition.comparator is not operator.ne
    raise ValueError("Invalid KeyCondition {}".format(condition))


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

    def __init__(self, engine, model, index=None):
        self.engine = engine
        self.model = model
        self.index = index

        self._key_condition = None
        self._filter_condition = None
        self._select = "all"
        self._forward = True
        self._consistent = False

        self._select_columns = []

    def copy(self):
        cls = self.__class__
        other = cls(engine=self.engine, model=self.model, index=self.index)

        for attr in ["_filter_condition", "_key_condition",
                     "_select", "_forward", "_consistent"]:
            setattr(other, attr, getattr(self, attr))

        other._select_columns = list(self._select_columns)
        other._key_condition = self._key_condition

        return other

    def key(self, condition):
        # AND multiple conditions
        if self._key_condition:
            condition &= self._key_condition

        # a hash condition is always required; a range condition
        # is allowed if the table/index has a range
        if self.index:
            hash_column = self.index.hash_key
            range_column = self.index.range_key
        else:
            hash_column = self.model.hash_key
            range_column = self.model.range_key

        max_conditions = 1
        if range_column:
            max_conditions += 1

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
        columns is REQUIRED when mode is 'specific', and columns must be a list
        '''
        # Can't just copy and extend in case this is an
        # empty select() to explicitly select all columns
        other = self.copy()
        if not columns:
            other._select_columns.clear()
            other._select = "all"
        else:
            other._select_columns.extend(columns)
            other._select = "specific"
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
        call = getattr(self.engine.dynamo_client, self.filter_type)
        request = self.generate_request()
        return FilterResult(prefetch, call, request, self.engine, self.model)

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


class Query(Filter):
    filter_type = "query"

    def generate_request(self):
        meta = self.model.__meta__
        request = {
            'TableName': meta['dynamo.table.name'],
            'Select': SELECT_MODES[self._select],
            'ScanIndexForward': self._forward,
            'ConsistentRead': self._consistent
        }

        if not self._key_condition:
            raise ValueError("Must specify at least a hash key condition")

        if self.index:
            request['IndexName'] = self.index.dynamo_name
            if self._consistent and bloop.column.is_global_index(self.index):
                raise ValueError(
                    "Cannot use ConsistentRead with a GlobalSecondaryIndex")

        # Render key and filter conditions
        renderer = bloop.condition.ConditionRenderer(
            self.engine, self.model, legacy=False)

        request.update(renderer.render(self._key_condition, mode="key"))
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


class Scan(Filter):
    filter_type = "scan"

    def generate_request(self):
        meta = self.model.__meta__
        request = {
            'TableName': meta['dynamo.table.name'],
            'Select': SELECT_MODES[self._select]
        }

        if self.index:
            request['IndexName'] = self.index.dynamo_name

        # Render key and filter conditions
        renderer = bloop.condition.ConditionRenderer(
            self.engine, self.model, legacy=False)

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


class FilterResult(object):
    '''
    Result from a scan or query.  Usually lazy loaded, iterate to execute.

    Uses engine.prefetch to control call batching
    '''
    def __init__(self, prefetch, call, request, engine, model):
        self._call = call
        self._prefetch = prefetch
        self._request = request
        self.engine = engine
        self.model = model

        # We'll need to access these to load each result
        meta = model.__meta__
        self._columns = meta["dynamo.columns"]
        self._init = meta["bloop.init"]

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
            return self.__prefetch_iter__

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
        response = self._call(**self._request)
        self._continue = response.get("LastEvaluatedKey", None)

        self.count += response["Count"]
        self.scanned_count += response["ScannedCount"]

        results = response.get("Items", [])
        for result in results:
            obj = self.engine.__load__(self.model, result)
            self._results.append(obj)
            yield obj
