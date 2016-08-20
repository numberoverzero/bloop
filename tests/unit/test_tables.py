import pytest
from bloop.session import (
    create_table_request,
    expected_table_description,
    ready,
    sanitized_table_description,
    simple_table_status,
)
from bloop.util import ordered

from ..helpers.models import ComplexModel, SimpleModel, User


def assert_unordered(obj, other):
    assert ordered(obj) == ordered(other)


def test_create_simple():
    expected = {
        'AttributeDefinitions': [
            {'AttributeName': 'id', 'AttributeType': 'S'}],
        'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1},
        'TableName': 'Simple'}
    assert_unordered(create_table_request(SimpleModel), expected)


def test_create_complex():
    expected = {
        'AttributeDefinitions': [
            {'AttributeType': 'S', 'AttributeName': 'date'},
            {'AttributeType': 'S', 'AttributeName': 'email'},
            {'AttributeType': 'S', 'AttributeName': 'joined'},
            {'AttributeType': 'S', 'AttributeName': 'name'}],
        'GlobalSecondaryIndexes': [{
            'IndexName': 'by_email',
            'KeySchema': [{'KeyType': 'HASH', 'AttributeName': 'email'}],
            'Projection': {'ProjectionType': 'ALL'},
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 4, 'WriteCapacityUnits': 5}}],
        'KeySchema': [{'KeyType': 'HASH', 'AttributeName': 'name'},
                      {'KeyType': 'RANGE', 'AttributeName': 'date'}],
        'LocalSecondaryIndexes': [{
            'IndexName': 'by_joined',
            'KeySchema': [
                {'KeyType': 'HASH', 'AttributeName': 'name'},
                {'KeyType': 'RANGE', 'AttributeName': 'joined'}],
            'Projection': {
                'NonKeyAttributes': ['joined', 'email', 'date', 'name'],
                'ProjectionType': 'INCLUDE'}}],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 3, 'WriteCapacityUnits': 2},
        'TableName': 'CustomTableName'}
    assert_unordered(create_table_request(ComplexModel), expected)


def test_expected_description():
    # Eventually expected_table_description will probably diverge from create_table
    # This will guard against (or coverage should show) if there's drift
    create = create_table_request(ComplexModel)
    expected = expected_table_description(ComplexModel)
    assert_unordered(create, expected)


def test_sanitize_drop_empty_lists():
    expected = expected_table_description(ComplexModel)
    # Start from the same base, but inject an unnecessary NonKeyAttributes
    description = expected_table_description(ComplexModel)
    index = description["GlobalSecondaryIndexes"][0]
    index["Projection"]["NonKeyAttributes"] = []

    assert_unordered(expected, sanitized_table_description(description))


def test_sanitize_drop_empty_indexes():
    expected = expected_table_description(SimpleModel)
    # Start from the same base, but inject an unnecessary NonKeyAttributes
    description = expected_table_description(SimpleModel)
    description["GlobalSecondaryIndexes"] = []

    assert_unordered(expected, sanitized_table_description(description))


def test_sanitize_expected():
    expected = expected_table_description(User)
    # Add some extra fields
    description = {
        'AttributeDefinitions': [
            {'AttributeType': 'S', 'AttributeName': 'email'},
            {'AttributeType': 'S', 'AttributeName': 'id'}],
        'CreationDateTime': 'EXTRA_FIELD',
        'ItemCount': 'EXTRA_FIELD',
        'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
        'GlobalSecondaryIndexes': [{
            'IndexArn': 'EXTRA_FIELD',
            'IndexName': 'by_email',
            'IndexSizeBytes': 'EXTRA_FIELD',
            'IndexStatus': 'EXTRA_FIELD',
            'KeySchema': [{'AttributeName': 'email', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'},
            'ProvisionedThroughput': {
                'NumberOfDecreasesToday': 'EXTRA_FIELD',
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1}}],
        'ProvisionedThroughput': {
            'LastDecreaseDateTime': 'EXTRA_FIELD',
            'LastIncreaseDateTime': 'EXTRA_FIELD',
            'NumberOfDecreasesToday': 'EXTRA_FIELD',
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1},
        'TableArn': 'EXTRA_FIELD',
        'TableName': 'User',
        'TableSizeBytes': 'EXTRA_FIELD',
        'TableStatus': 'EXTRA_FIELD'}
    sanitized = sanitized_table_description(description)
    assert_unordered(expected, sanitized)


@pytest.mark.parametrize("table_status, gsi_status, expected_status", [
    ("ACTIVE", "ACTIVE", ready),
    ("ACTIVE", None, ready),
    ("ACTIVE", "BUSY", None),
    ("BUSY", "ACTIVE", None),
    ("BUSY", "BUSY", None)
])
def test_simple_status(table_status, gsi_status, expected_status):
    """Status is busy because table isn't ACTIVE, no GSIs"""
    description = {"TableStatus": table_status}
    if gsi_status is not None:
        description["GlobalSecondaryIndexes"] = [{"IndexStatus": gsi_status}]
    assert simple_table_status(description) == expected_status
