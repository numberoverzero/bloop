from bloop import (
    new_base, Column, DateTime, Float, GlobalSecondaryIndex,
    Integer, List, LocalSecondaryIndex, Map, String, UUID)

DocumentType = Map(**{
    'Rating': Float(),
    'Stock': Integer(),
    'Description': Map(**{
        'Heading': String,
        'Body': String,
        'Specifications': String
    }),
    'Id': UUID,
    'Updated': DateTime
})
BaseModel = new_base()


class Document(BaseModel):
    id = Column(Integer, hash_key=True)
    data = Column(DocumentType)
    numbers = Column(List(Integer))


class User(BaseModel):
    id = Column(UUID, hash_key=True)
    age = Column(Integer)
    name = Column(String)
    email = Column(String)
    joined = Column(DateTime, name="j")
    by_email = GlobalSecondaryIndex(
        hash_key="email", projection="all")


class ComplexModel(BaseModel):
    class Meta:
        write_units = 2
        read_units = 3
        table_name = "CustomTableName"

    name = Column(UUID, hash_key=True)
    date = Column(String, range_key=True)
    email = Column(String)
    joined = Column(String)
    not_projected = Column(Integer)
    by_email = GlobalSecondaryIndex(
        hash_key="email", read_units=4, projection="all", write_units=5)
    by_joined = LocalSecondaryIndex(
        range_key="joined", projection=["email"])
