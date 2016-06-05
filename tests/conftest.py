import bloop
import botocore
import contextlib
import pytest


DocumentType = bloop.Map(**{
    'Rating': bloop.Float(),
    'Stock': bloop.Integer(),
    'Description': bloop.Map(**{
        'Heading': bloop.String,
        'Body': bloop.String,
        'Specifications': bloop.String
    }),
    'Id': bloop.UUID,
    'Updated': bloop.DateTime
})


def noop(*a, **kw):
    pass


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
def engine(client):
    engine = bloop.Engine()
    engine.client = client
    return engine


@pytest.fixture
def atomic(engine):
    with engine.context(atomic=True) as atomic:
        return atomic


@pytest.fixture
def renderer(engine):
    return bloop.condition.ConditionRenderer(engine)


@pytest.fixture
def local_bind(engine):
    @contextlib.contextmanager
    def context():
        real_create_table = engine.client.create_table
        real_validate_table = engine.client.validate_table
        engine.client.create_table = noop
        engine.client.validate_table = noop
        yield
        engine.client.create_table = real_create_table
        engine.client.validate_table = real_validate_table
    return context


@pytest.fixture
def base_model():
    return bloop.new_base()


@pytest.fixture
def UnboundUser(engine, base_model):
    class User(base_model):
        id = bloop.Column(bloop.UUID, hash_key=True)
        age = bloop.Column(bloop.Integer)
        name = bloop.Column(bloop.String)
        email = bloop.Column(bloop.String)
        # Field with dynamo_name != model_name
        joined = bloop.Column(bloop.DateTime, name="j")

        by_email = bloop.GlobalSecondaryIndex(hash_key="email",
                                              projection="all")
    return User


@pytest.fixture
def User(UnboundUser, engine, base_model, local_bind):
    with local_bind():
        engine.bind(base=base_model)
    return UnboundUser


@pytest.fixture
def ComplexModel(engine, base_model, local_bind):
    class Model(base_model):
        class Meta:
            write_units = 2
            read_units = 3
            table_name = "CustomTableName"
        name = bloop.Column(bloop.UUID, hash_key=True)
        date = bloop.Column(bloop.String, range_key=True)
        email = bloop.Column(bloop.String)
        joined = bloop.Column(bloop.String)
        not_projected = bloop.Column(bloop.Integer)

        by_email = bloop.GlobalSecondaryIndex(hash_key="email", read_units=4,
                                              projection="all", write_units=5)
        by_joined = bloop.LocalSecondaryIndex(range_key="joined",
                                              projection=["email"])
    with local_bind():
        engine.bind(base=base_model)
    return Model


@pytest.fixture
def document_type():
    return DocumentType


@pytest.fixture
def Document(engine, base_model, local_bind):
    class Document(base_model):
        id = bloop.Column(bloop.Integer, hash_key=True)
        data = bloop.Column(DocumentType)
        numbers = bloop.Column(bloop.List(bloop.Integer))
    with local_bind():
        engine.bind(base=base_model)
    return Document


@pytest.fixture
def SimpleModel(engine, base_model, local_bind):
    class Model(base_model):
        id = bloop.Column(bloop.UUID, hash_key=True)
    with local_bind():
        engine.bind(base=base_model)
    return Model


@pytest.fixture
def client_error():
    def _client_error(code):
        error_response = {"Error": {
            "Code": code,
            "Message": "FooMessage"}}
        operation_name = "OperationName"
        return botocore.exceptions.ClientError(error_response, operation_name)
    return _client_error
