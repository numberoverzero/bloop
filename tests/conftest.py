import bloop
import botocore
import pytest


def noop(*a, **kw):
    pass


@pytest.fixture
def ordered():
    def ordered(obj):
        '''
        Return sorted version of nested dicts/lists for comparing.

        http://stackoverflow.com/a/25851972
        '''
        if isinstance(obj, dict):
            return sorted((k, ordered(v)) for k, v in obj.items())
        if isinstance(obj, list):
            return sorted(ordered(x) for x in obj)
        else:
            return obj
    return ordered


@pytest.fixture
def client(session):
    return bloop.client.Client(session=session)


@pytest.fixture
def session():
    class DummyClient:
        pass

    class DummySession:
        def client(self, name):
            assert name == "dynamodb"
            return DummyClient()
    return DummySession()


@pytest.fixture
def engine(session):
    return bloop.Engine(session=session)


@pytest.fixture
def renderer(engine):
    return bloop.condition.ConditionRenderer(engine)


@pytest.fixture
def local_bind(engine):
    engine.client.create_table = noop
    engine.client.validate_table = noop


@pytest.fixture
def UnboundUser(engine):
    class User(engine.model):
        id = bloop.Column(bloop.UUID, hash_key=True)
        age = bloop.Column(bloop.Integer)
        name = bloop.Column(bloop.String)
        email = bloop.Column(bloop.String)
        joined = bloop.Column(bloop.DateTime)

        by_email = bloop.GlobalSecondaryIndex(hash_key='email',
                                              projection='all')
    return User


@pytest.fixture
def User(UnboundUser, engine, local_bind):
    engine.bind()
    return UnboundUser


@pytest.fixture
def ComplexModel(engine, local_bind):
    class Model(engine.model):
        class Meta:
            write_units = 2
            read_units = 3
            table_name = 'CustomTableName'
        name = bloop.Column(bloop.UUID, hash_key=True)
        date = bloop.Column(bloop.DateTime, range_key=True)
        email = bloop.Column(bloop.String)
        joined = bloop.Column(bloop.DateTime)

        by_email = bloop.GlobalSecondaryIndex(hash_key='email', read_units=4,
                                              projection='all', write_units=5)
        by_joined = bloop.LocalSecondaryIndex(range_key='joined',
                                              projection=['email'])
    engine.bind()
    return Model


@pytest.fixture
def SimpleModel(engine, local_bind):
    class Model(engine.model):
        id = bloop.Column(bloop.UUID, hash_key=True)
    engine.bind()
    return Model


@pytest.fixture
def client_error():
    def ClientError(code):
        error_response = {'Error': {
            'Code': code,
            'Message': 'FooMessage'}}
        operation_name = 'OperationName'
        return botocore.exceptions.ClientError(error_response, operation_name)
    return ClientError
