missing = object()


class GlobalSecondaryIndex(object):
    def __init__(self, hash_key=None, range_key=None,
                 write_units=1, read_units=1, name=missing):
        self._model_name = None
        self._backing_name = name

        self.write_units = write_units
        self.read_units = read_units
        self.hash_key = hash_key
        self.range_key = range_key

    @property
    def model_name(self):
        ''' Name of the model's attr that references self '''
        return self._model_name

    @property
    def dynamo_name(self):
        if self._backing_name is missing:
            return self.model_name
        return self._backing_name
