import random
import string
from enum import Enum

from bloop import (
    UUID,
    BaseModel,
    Boolean,
    Column,
    DateTime,
    DynamicMap,
    GlobalSecondaryIndex,
    LocalSecondaryIndex,
    Set,
    String,
)


# ================================================================================
# Types
# ================================================================================
class StringEnum(String):
    def __init__(self, enum_cls):
        self.enum_cls = enum_cls
        super().__init__()

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return value
        value = value.name
        return super().dynamo_dump(value, context=context, **kwargs)

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return value
        value = super().dynamo_load(value, context=context, **kwargs)
        return self.enum_cls[value]


class Role(Enum):
    user = "user"
    curator = "curator"
    superuser = "super_user"
    admin = "admin"


# ================================================================================
# Mixins
# ================================================================================
class UUIDHashKey(object):
    id = Column(UUID, hash_key=True)


class CreatedRangeKey(object):
    created = Column(DateTime, range_key=True)


class IdentityMixin(object):
    roles = Column(Set(StringEnum(Role)))

    @property
    def is_active(self):
        return True

    @property
    def is_authenticated(self):
        return True

    @property
    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id


# ================================================================================
# Classes
# ================================================================================
class MixinBase(BaseModel, UUIDHashKey, CreatedRangeKey):
    class Meta:
        abstract = True
    email = Column(String)
    updated = Column(DateTime)
    active = Column(Boolean)
    by_created = LocalSecondaryIndex(projection="all", range_key=CreatedRangeKey.created)


class MixinUser(MixinBase, IdentityMixin):
    first_name = Column(String)
    last_name = Column(String)
    by_email = GlobalSecondaryIndex(projection='all', dynamo_name='email-index', hash_key='email')

    def __str__(self):
        return "{} {}: {}".format(self.first_name, self.last_name, self.email)


class ExternalUser(MixinUser):
    company = Column(String)
    by_email = GlobalSecondaryIndex(projection='all', dynamo_name='email-index', hash_key=MixinUser.email)


class User(BaseModel):
    class Meta:
        read_units = 1
        write_units = 3
    email = Column(String, hash_key=True)
    username = Column(String, range_key=True)
    by_username = GlobalSecondaryIndex(projection="keys", hash_key="username")

    profile = Column(String)
    data = Column(DynamicMap)
    extra = Column(String)


def _letters(n):
    return "".join(random.choice(string.ascii_letters) for _ in range(n))


def valid_user():
    email = "e-{}@{}".format(_letters(3), _letters(4))
    username = "u-{}".format(_letters(7))
    return User(
        email=email,
        username=username
    )
