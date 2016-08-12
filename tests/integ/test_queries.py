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
