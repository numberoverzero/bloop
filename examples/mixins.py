import base64
import datetime
import hashlib

import delorean

from bloop import BaseModel, Binary, Column, Engine, Integer, String
from bloop.ext.delorean import Timestamp


DEFAULT_PASTE_LIFETIME_DAYS = 31


def new_expiry(days=DEFAULT_PASTE_LIFETIME_DAYS):
    """Return an expiration `days` in the future"""
    now = delorean.Delorean()
    return now + datetime.timedelta(days=days)


class SortByVersion:
    """Mixin for a string-based hash key and a version number for range_key"""
    id = Column(String, hash_key=True)
    version = Column(Integer, range_key=True, dynamo_name="v")


class Paste(SortByVersion, BaseModel):
    class Meta:
        ttl = {"column": "not_after"}

    not_after = Column(Timestamp, default=new_expiry)
    bucket = Column(String, dynamo_name="b")
    key = Column(String, dynamo_name="k")


class UserImage(SortByVersion, BaseModel):
    jpg = Column(Binary)


engine = Engine()
engine.bind(BaseModel)


def s3_upload(content: str) -> (str, str):
    # TODO persist in s3
    return "bucket-id", "key-id"


def b64sha256(content: str) -> str:
    hash = hashlib.sha256(content.encode())
    return base64.b64encode(hash.digest()).decode()


def new_paste(content: str) -> str:
    id = b64sha256(content)
    bucket, key = s3_upload(content)

    paste = Paste(bucket=bucket, key=key, id=id, version=0)
    engine.save(paste)
    return id


def get_paste(id: str, version=None) -> Paste:
    if version:
        paste = Paste(id=id, version=version)
        engine.load(paste)
        return paste
    else:
        # Reverse ordering to get last value of version
        query = engine.query(Paste, key=Paste.id == id, forward=False)
        return query.first()
