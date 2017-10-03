import arrow

from .. import types


DEFAULT_TIMEZONE = "utc"


class DateTime(types.DateTime):
    python_type = arrow.Arrow

    def __init__(self, timezone=DEFAULT_TIMEZONE):
        self.timezone = timezone
        super().__init__()

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        value = value.to("utc").datetime
        return super().dynamo_dump(value, context=context, **kwargs)

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        dt = super().dynamo_load(value, context=context, **kwargs)
        return arrow.get(dt).to(self.timezone)


class Timestamp(types.Timestamp):
    python_type = arrow.Arrow

    def __init__(self, timezone=DEFAULT_TIMEZONE):
        self.timezone = timezone
        super().__init__()

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        value = value.to("utc").datetime
        return super().dynamo_dump(value, context=context, **kwargs)

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        dt = super().dynamo_load(value, context=context, **kwargs)
        return arrow.get(dt).to(self.timezone)
