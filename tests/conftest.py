import bloop
import bloop.client
import pytest
from unittest.mock import Mock

BaseModel = bloop.new_base()
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


class DocumentModel(BaseModel):
    id = bloop.Column(bloop.Integer, hash_key=True)
    data = bloop.Column(DocumentType)
    numbers = bloop.Column(bloop.List(bloop.Integer))


class UserModel(BaseModel):
    class Meta:
        table_name = "User"
    id = bloop.Column(bloop.UUID, hash_key=True)
    age = bloop.Column(bloop.Integer)
    name = bloop.Column(bloop.String)
    email = bloop.Column(bloop.String)
    # Field with dynamo_name != model_name
    joined = bloop.Column(bloop.DateTime, name="j")
    by_email = bloop.GlobalSecondaryIndex(hash_key="email", projection="all")


class Model(BaseModel):
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


@pytest.fixture
def engine():
    engine = bloop.Engine()
    engine.client = Mock(spec=bloop.client.Client)
    engine.bind(base=BaseModel)
    return engine


@pytest.fixture
def atomic(engine):
    with engine.context(atomic=True) as atomic:
        return atomic


@pytest.fixture
def User():
    return UserModel


@pytest.fixture
def ComplexModel():
    return Model


@pytest.fixture
def document_type():
    return DocumentType


@pytest.fixture
def Document():
    return DocumentModel
