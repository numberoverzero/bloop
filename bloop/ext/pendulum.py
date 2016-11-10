import datetime

import pendulum

from .. import types


DEFAULT_TIMEZONE = datetime.timezone.utc


class DateTime(types.DateTime):
    python_type = pendulum.Pendulum

    def __init__(self, timezone=DEFAULT_TIMEZONE):
        self.timezone = timezone
        super().__init__()

    def dynamo_dump(self, value, *, context, **kwargs):
        value = value.in_timezone("utc")
        return super().dynamo_dump(value, context=context, **kwargs)

    def dynamo_load(self, value, *, context, **kwargs):
        dt = super().dynamo_load(value, context=context, **kwargs)
        return pendulum.instance(dt).in_timezone(self.timezone)
