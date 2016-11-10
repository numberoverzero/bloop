import pytest
import delorean
from bloop.types import FIXED_ISO8601_FORMAT
from datetime import datetime
import pytz

from bloop.ext.delorean import DateTime


now = datetime.now(pytz.utc)
now_eastern = datetime.now(pytz.timezone("US/Eastern"))
now_iso8601 = now.strftime(FIXED_ISO8601_FORMAT)


@pytest.mark.parametrize("timezone", ["utc", "US/Eastern"])
def test_datetime(timezone):
    delorean_now = delorean.Delorean(now)
    typedef = DateTime(timezone)

    assert typedef.dynamo_dump(delorean_now, context={}) == now_iso8601
    assert typedef.dynamo_load(now_iso8601, context={}).shift("utc").datetime == now
