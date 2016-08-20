from .models import User, valid_user


def test_query_with_projection(engine):
    engine.bind(User)
    user = valid_user()
    user.profile = "Hello, World"
    engine.save(user)

    query = engine.query(
        User,
        key=User.email == user.email,
        projection={User.email, User.username})

    result = query.one()
    assert not hasattr(result, "profile")


def test_limit(engine):
    engine.bind(User)
    users = [valid_user() for _ in range(10)]
    engine.save(*users)

    scan = engine.scan(User, limit=3)
    results = list(scan)

    assert len(results) == 3
    assert scan.count == 10
    assert scan.scanned == 10
    assert scan.exhausted
