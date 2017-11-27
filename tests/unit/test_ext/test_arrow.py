from datetime import datetime

import arrow
import pytest
import pytz

from bloop.ext.arrow import DateTime, Timestamp
from bloop.types import FIXED_ISO8601_FORMAT


now = datetime.now(pytz.utc).replace(microsecond=0)
now_eastern = datetime.now(pytz.timezone("US/Eastern"))
now_iso8601 = now.strftime(FIXED_ISO8601_FORMAT)
now_timestamp = str(int(now.timestamp()))


@pytest.mark.parametrize("timezone", ["utc", "US/Eastern"])
def test_datetime(timezone):
    arrow_now = arrow.get(now)
    typedef = DateTime(timezone)

    assert typedef.dynamo_dump(arrow_now, context={}) == now_iso8601
    assert typedef.dynamo_load(now_iso8601, context={}).to("utc").datetime == now


@pytest.mark.parametrize("timezone", ["utc", "US/Eastern"])
def test_timestamp(timezone):
    arrow_now = arrow.get(now)
    typedef = Timestamp(timezone)

    assert typedef.dynamo_dump(arrow_now, context={}) == now_timestamp
    assert typedef.dynamo_load(now_timestamp, context={}).to("utc").datetime == now


@pytest.mark.parametrize("typedef_cls", (DateTime, Timestamp))
def test_none(typedef_cls):
    typedef = typedef_cls()
    assert typedef.dynamo_dump(None, context={}) is None
    assert typedef.dynamo_load(None, context={}) is None
