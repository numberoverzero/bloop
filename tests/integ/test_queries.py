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
    scan = engine.scan(User, projection="count")

    for _ in range(7):
        engine.save(valid_user())
    assert scan.count == 7

    scan.reset()
    engine.save(valid_user())
    assert scan.count == 8


def test_first_limits_to_one(engine):
    engine.bind(User)
    for _ in range(5):
        engine.save(valid_user())

    scan = engine.scan(User)
    user = scan.first()
    assert scan.exhausted is False
    assert scan.count == 1


def test_one_limits_to_two(engine):
    engine.bind(User)
    saved = []
    for _ in range(5):
        user = valid_user()
        engine.save(user)
        saved.append(user)

    scan = engine.scan(User, filter=User.email == saved[0].email)
    user = scan.one()
    assert scan.exhausted is True
    assert scan.count == 1


def test_scan_all_with_limit(engine):
    engine.bind(User)
    for _ in range(19):
        engine.save(valid_user())

    assert engine.scan(User).count == 0
    scan = engine.scan(User).limit(10)

    # after first scan it should have only read in our limit
    next(scan)
    assert scan.count == 10

    # make sure that after iterating past the limit, it pulls the next 'page'
    [next(scan) for _ in range(10)]

    # at this point, it should have fetched all 19 users
    assert scan.count == 19

    # We still have items in the buffer however
    assert scan.exhausted is False

    # eat up the rest of the deque
    users = [user for user in scan]

    # make sure it was the last 8 users
    assert len(users) == 8
    assert scan.exhausted is True
