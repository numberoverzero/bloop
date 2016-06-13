import pytest

import bloop.tables
import bloop.util

from test_models import SimpleModel, ComplexModel, User


statuses = [
    ("ACTIVE", "ACTIVE", "ACTIVE"),
    ("ACTIVE", None, "ACTIVE"),
    ("ACTIVE", "BUSY", "BLOOP_NOT_ACTIVE"),
    ("BUSY", "ACTIVE", "BLOOP_NOT_ACTIVE"),
    ("BUSY", "BUSY", "BLOOP_NOT_ACTIVE")
]


def assert_unordered(obj, other):
    assert bloop.util.ordered(obj) == bloop.util.ordered(other)


def test_create_simple():
    expected = {
        'AttributeDefinitions': [
            {'AttributeName': 'id', 'AttributeType': 'S'}],
        'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1},
        'TableName': 'Simple'}
    assert_unordered(bloop.tables.create_request(SimpleModel), expected)


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
    assert_unordered(bloop.tables.create_request(ComplexModel), expected)


def test_expected_description():
    # Eventually expected_description will probably diverge from create_table
    # This will guard against (or coverage should show) if there's drift
    create = bloop.tables.create_request(ComplexModel)
    expected = bloop.tables.expected_description(ComplexModel)
    assert_unordered(create, expected)


def test_sanitize_drop_empty_lists():
    expected = bloop.tables.expected_description(ComplexModel)
    # Start from the same base, but inject an unnecessary NonKeyAttributes
    description = bloop.tables.expected_description(ComplexModel)
    index = description["GlobalSecondaryIndexes"][0]
    index["Projection"]["NonKeyAttributes"] = []

    assert_unordered(expected, bloop.tables.sanitized_description(description))


def test_sanitize_drop_empty_indexes():
    expected = bloop.tables.expected_description(SimpleModel)
    # Start from the same base, but inject an unnecessary NonKeyAttributes
    description = bloop.tables.expected_description(SimpleModel)
    description["GlobalSecondaryIndexes"] = []

    assert_unordered(expected, bloop.tables.sanitized_description(description))


def test_sanitize_expected():
    expected = bloop.tables.expected_description(User)
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
            'NumberOfDecreasesToday': 'EXTRA_FIELD',
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1},
        'TableArn': 'EXTRA_FIELD',
        'TableName': 'User',
        'TableSizeBytes': 'EXTRA_FIELD',
        'TableStatus': 'EXTRA_FIELD'}
    sanitized = bloop.tables.sanitized_description(description)
    assert_unordered(expected, sanitized)


@pytest.mark.parametrize("table_status, gsi_status, expected_status", statuses)
def test_simple_status(table_status, gsi_status, expected_status):
    """Status is busy because table isn't ACTIVE, no GSIs"""
    description = {"TableStatus": table_status}
    if gsi_status is not None:
        description["GlobalSecondaryIndexes"] = [{"IndexStatus": gsi_status}]
    assert bloop.tables.simple_status(description) == expected_status
