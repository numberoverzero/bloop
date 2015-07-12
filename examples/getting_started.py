"""
Combined source from the README's "Getting Started" section.
"""
from bloop import (Boolean, Engine, Column, DateTime,
                   GlobalSecondaryIndex, Integer, String, UUID)
import arrow
import uuid
engine = Engine()


class User(engine.model):
    id = Column(UUID, hash_key=True)
    admin = Column(Boolean, name='a')


class Post(engine.model):
    id = Column(UUID, hash_key=True)
    user = Column(UUID, name='u')

    date = Column(DateTime(timezone='US/Pacific'), name='d')
    views = Column(Integer, name='v')
    content = Column(String, name='c')

    by_user = GlobalSecondaryIndex(hash_key='user', projection='keys_only',
                                   write_units=1, read_units=10)
# engine.bind()


def create_user(admin=False):
    ''' Create a new user, throwing if the randomly generated id is in use '''
    user = User(id=uuid.uuid4(), admin=admin)
    does_not_exist = User.id.is_(None)
    engine.save(user, condition=does_not_exist)
    return user


def posts_by_user(user_id):
    ''' Returns an iterable of posts by the user '''
    return engine.query(Post, index=Post.by_user).key(Post.id == user_id)


def increment_views(post_id):
    '''
    Load post, increment views, save with the condition that the view count
    still has its old value
    '''
    post = Post(id=post_id)
    engine.load(post)
    post.views += 1
    old_views = Post.views == (post.views - 1)
    engine.save(post, condition=old_views)


def edit(user_id, post_id, new_content):
    ''' Verify user can edit post, then change content and update date '''
    user = User(id=user_id)
    post = Post(id=post_id)
    engine.load([user, post])

    if (not user.admin) and (post.user != user.id):
        raise ValueError("User not authorized to edit post.")

    post.content = new_content
    post.date = arrow.now()  # timezone doesn't matter, bloop stores in UTC
    engine.save(post)


def recent_posts_local_time(timezone, days_old):
    ''' ex: timezone='Europe/Paris', days_old=1 '''
    now_local = arrow.now().to(timezone)
    yesterday_local = now_local.replace(days=-days_old)

    since_yesterday = Post.date.between(yesterday_local, now_local)
    return engine.scan(Post).filter(since_yesterday)


def main():
    user = create_user(admin=True)
    post = Post(id=uuid.uuid4(), user=user.id, date=arrow.now(),
                views=0, content="Hello!")
    engine.save(post)
    increment_views(post.id)
    edit(user.id, post.id, "World!")
    for post in recent_posts_local_time("US/Pacific", 1):
        print(post)

if __name__ == "__main__":
    main()
