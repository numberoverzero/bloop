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


def test_scan_count(engine):
    engine.bind(User)
    for _ in range(7):
        engine.save(valid_user())
    scan = engine.scan(User, projection="count")
    assert scan.count == 0
    try:
        next(scan)
    except StopIteration:
        pass
    assert scan.count == 7
