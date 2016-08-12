"""Verify that travis' secure env variable are working by creating a table"""
import boto3
from bloop import Client, Engine, new_base, Column, Integer


boto_client = boto3.client("dynamodb", region_name="us-west-2")
bloop_client = Client(boto_client=boto_client)
engine = Engine(client=bloop_client)


class User(new_base()):
    id = Column(Integer, hash_key=True)
engine.bind(User)


def test_success():
    assert True
