import boto3.session
import pytest
import bloop


@pytest.fixture
def session():
    return boto3.session.Session(region_name='us-west-2',
                                 aws_access_key_id='ACCESS_KEY',
                                 aws_secret_access_key='SECRET_KEY')


@pytest.fixture
def engine(session):
    return bloop.Engine(session=session)
