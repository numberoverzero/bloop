import random
import uuid
from datetime import datetime, timezone
from string import ascii_letters

import pytest
from tests.integ.models import ExternalUser, MixinUser, Role

from bloop import (
    UUID,
    BaseModel,
    Column,
    DateTime,
    GlobalSecondaryIndex,
    Integer,
)
from bloop.exceptions import InvalidModel


def test_inheritance_simple(engine):
    class NewBase(BaseModel):
        class Meta:
            abstract = True
        uuid = Column(UUID)

    class SimpleModel(NewBase):
        id = Column(Integer, hash_key=True)
        created_at = Column(DateTime)

    model = SimpleModel()
    assert len(model.Meta.columns) == 3
    assert len(model.Meta.keys) == 1
    assert list(model.Meta.keys)[0].name == 'id'


def test_inheritance_base_hashkey(engine):
    class NewBase(BaseModel):
        class Meta:
            abstract = True
        uuid = Column(UUID, hash_key=True)

    class SimpleModel(NewBase):
        id = Column(Integer)
        created_at = Column(DateTime)

    model = SimpleModel()
    assert len(model.Meta.columns) == 3
    assert len(model.Meta.keys) == 1
    assert list(model.Meta.keys)[0].name == 'uuid'


def test_inheritance_mixins(engine):
    model = MixinUser()
    assert len(model.Meta.columns) == 8
    assert len(model.Meta.keys) == 2
    assert model.Meta.hash_key.name == 'id'
    assert model.Meta.range_key.name == 'created'


def _create_user(cls, **extra):
    now = datetime.now(timezone.utc)
    first_name = "".join([random.choice(ascii_letters) for _ in range(8)])
    last_name = "".join([random.choice(ascii_letters) for _ in range(12)])
    email = f"{first_name}.{last_name}@example.com"

    return cls(
        id=uuid.uuid4(), created=now, updated=now, active=True,
        first_name=first_name, last_name=last_name, email=email,
        **extra
    )


def gen_external_user():
    extra = {'company': 'Acme', 'roles': {Role.user, Role.admin}}
    return _create_user(ExternalUser, **extra)


def gen_mixin_user():
    extra = {'roles': {Role.user}}
    return _create_user(MixinUser, **extra)


@pytest.mark.parametrize("cls, factory", [
    (MixinUser, gen_mixin_user),
    (ExternalUser, gen_external_user)
])
def test_inheritance_load(engine, cls, factory):
    engine.bind(BaseModel)

    obj = factory()
    engine.save(obj)

    same_obj = cls(id=obj.id, created=obj.created)
    engine.load(same_obj)

    assert same_obj.Meta.model is cls

    for attr in [col.name for col in obj.Meta.columns]:
        assert getattr(same_obj, attr) == getattr(obj, attr)


def test_inheritance_lsi_from_baseclass(engine):
    engine.bind(BaseModel)

    first_group = []
    for x in range(3):
        user = gen_mixin_user()
        engine.save(user)
        first_group.append(user)

    saved_date = datetime.now(timezone.utc)

    second_group = []
    for x in range(3):
        user = gen_mixin_user()
        engine.save(user)
        second_group.append(user)

    # ensure that we won't find a user in the first group that has a created after our saved date.
    cond = (MixinUser.id == first_group[0].id) & (MixinUser.created > saved_date)
    q = engine.query(MixinUser.by_created, key=cond)
    assert len(list(q)) == 0

    # ensure that we *do* find a user in the second group that has a created after our saved date.
    cond = (MixinUser.id == second_group[-1].id) & (MixinUser.created > saved_date)
    q = engine.query(MixinUser.by_created, key=cond)
    items = list(q)
    assert len(items) == 1
    assert items[0].Meta.model is MixinUser


def test_inheritance_lsi_from_concrete_subclass(engine):
    engine.bind(BaseModel)

    first_group = []
    for x in range(3):
        user = gen_external_user()
        engine.save(user)
        first_group.append(user)

    saved_date = datetime.now(timezone.utc)

    second_group = []
    for x in range(3):
        user = gen_external_user()
        engine.save(user)
        second_group.append(user)

    # ensure that we won't find a user in the first group that has a created after our saved date.
    cond = (ExternalUser.id == first_group[0].id) & (ExternalUser.created > saved_date)
    q = engine.query(ExternalUser.by_created, key=cond)
    assert len(list(q)) == 0

    # ensure that we *do* find a user in the second group that has a created after our saved date.
    cond = (ExternalUser.id == second_group[-1].id) & (ExternalUser.created > saved_date)
    q = engine.query(ExternalUser.by_created, key=cond)
    items = list(q)
    assert len(items) == 1
    assert items[0].Meta.model is ExternalUser


def test_inheritance_gsi_to_baseclass(engine):
    engine.bind(BaseModel)

    user1 = gen_mixin_user()
    engine.save(user1)

    cond = MixinUser.email == user1.email
    user2 = engine.query(MixinUser.by_email, key=cond).one()

    assert user2.Meta.model is MixinUser
    for attr in [col.name for col in user1.Meta.columns]:
        assert getattr(user2, attr) == getattr(user1, attr)


def test_inheritance_gsi_from_concrete_subclass(engine):
    engine.bind(BaseModel)

    user1 = gen_external_user()
    engine.save(user1)

    cond = ExternalUser.email == user1.email
    user2 = engine.query(ExternalUser.by_email, key=cond).one()

    assert user2.Meta.model is ExternalUser
    for attr in [col.name for col in user1.Meta.columns]:
        assert getattr(user2, attr) == getattr(user1, attr)


def test_inheritance_overwrites_rangekey(engine):
    class NextGenUser(MixinUser):
        version = Column(Integer, range_key=True)


def test_inheritance_overwrites_hashkey(engine):
    class NextGenUser(MixinUser):
        version = Column(Integer, hash_key=True)


def test_inheritance_two_models_same_dynamo_index_name(engine):
    class NextGenUser(MixinUser):
        version = Column(Integer)
        next_by_email = GlobalSecondaryIndex(projection='all', dynamo_name='email-index', hash_key='email')


def test_inheritance_two_models_same_dynamo_column_name(engine):
    with pytest.raises(InvalidModel):
        class NextGenUser(MixinUser):
            version = Column(Integer, dynamo_name='email')
