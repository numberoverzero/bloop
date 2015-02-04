class Index(object):
    def __init__(self, write_units=1, read_units=1, name=None, range_key=None):
        self._model_name = None
        self._dynamo_name = name

        self.write_units = write_units
        self.read_units = read_units
        self.range_key = range_key

    @property
    def model_name(self):
        ''' Name of the model's attr that references self '''
        return self._model_name

    @model_name.setter
    def model_name(self, value):
        if self._model_name is not None:
            raise AttributeError("{} model_name already set to '{}'".format(
                self.__class__.__name__, self._model_name))
        self._model_name = value

    @property
    def dynamo_name(self):
        if self._dynamo_name is None:
            return self.model_name
        return self._dynamo_name


class GlobalSecondaryIndex(Index):
    def __init__(self, hash_key=None, **kwargs):
        super().__init__(**kwargs)
        self.hash_key = hash_key


class LocalSecondaryIndex(Index):
    ''' when constructing a model, you MUST set this index's model attr. '''
    @property
    def hash_key(self):
        hash_column = self.model.__meta__['dynamo.table.hash_key']
        return hash_column.dynamo_name
