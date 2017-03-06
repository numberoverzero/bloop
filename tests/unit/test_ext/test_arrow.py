import pytest
import arrow
from bloop.types import FIXED_ISO8601_FORMAT
from datetime import datetime
import pytz

from bloop.ext.arrow import DateTime


now = datetime.now(pytz.utc)
now_eastern = datetime.now(pytz.timezone("US/Eastern"))
now_iso8601 = now.strftime(FIXED_ISO8601_FORMAT)


@pytest.mark.parametrize("timezone", ["utc", "US/Eastern"])
def test_datetime(timezone):
    arrow_now = arrow.get(now)
    typedef = DateTime(timezone)

    assert typedef.dynamo_dump(arrow_now, context={}) == now_iso8601
    assert typedef.dynamo_load(now_iso8601, context={}).to("utc").datetime == now


def test_none():
    typedef = DateTime()
    assert typedef.dynamo_dump(None, context={}) is None
    assert typedef.dynamo_load(None, context={}) is None
