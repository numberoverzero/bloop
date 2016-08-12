from .models import User, valid_user


def test_query_with_select_subset(engine):
    engine.bind(User)
    user = valid_user()
    user.profile = "Hello, World"
    engine.save(user)

    query = engine.query(User)
    query.key = User.email == user.email
    query.select = {User.email, User.username}

    result = query.one()
    assert not hasattr(result, "profile")


def test_limit(engine):
    engine.bind(User)
    users = [valid_user() for _ in range(10)]
    engine.save(*users)

    scan = engine.scan(User)
    scan.limit = 3

    it = scan.build()
    results = list(it)

    assert len(results) == 3
    assert it.count == 10
    assert it.scanned == 10
    assert it.exhausted
