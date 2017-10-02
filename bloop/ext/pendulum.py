import pendulum

from .. import types


# https://github.com/sdispater/pendulum/issues/97
DEFAULT_TIMEZONE = "utc"


class DateTime(types.DateTime):
    python_type = pendulum.Pendulum

    def __init__(self, timezone=DEFAULT_TIMEZONE):
        self.timezone = timezone
        super().__init__()

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        value = value.in_timezone("utc")
        return super().dynamo_dump(value, context=context, **kwargs)

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        dt = super().dynamo_load(value, context=context, **kwargs)
        return pendulum.instance(dt).in_timezone(self.timezone)


class Timestamp(types.Timestamp):
    python_type = pendulum.Pendulum

    def __init__(self, timezone=DEFAULT_TIMEZONE):
        self.timezone = timezone
        super().__init__()

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        value = value.in_timezone("utc")
        return super().dynamo_dump(value, context=context, **kwargs)

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        dt = super().dynamo_load(value, context=context, **kwargs)
        return pendulum.instance(dt).in_timezone(self.timezone)
