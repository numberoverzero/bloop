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
