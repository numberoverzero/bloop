import bloop
import pytest


def noop(*a, **kw):
    pass


@pytest.fixture
def session():
    class Client:
        pass

    class Session:
        def client(self, name):
            assert name == "dynamodb"
            return Client()
    return Session()


@pytest.fixture
def engine(session):
    return bloop.Engine(session=session)


@pytest.fixture
def local_bind(engine):
    engine.client.create_table = noop
    engine.client.validate_table = noop


@pytest.fixture
def User(engine, local_bind):
    class User(engine.model):
        id = bloop.Column(bloop.UUID, hash_key=True)
        age = bloop.Column(bloop.Integer)
        name = bloop.Column(bloop.String)
        email = bloop.Column(bloop.String)
        joined = bloop.Column(bloop.DateTime)

        by_email = bloop.GlobalSecondaryIndex(hash_key='email',
                                              projection='all')
    engine.bind()
    return User
