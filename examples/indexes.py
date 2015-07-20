"""
Combined source from the README "Local and Global Secondary Indexes" section.
"""
from bloop import (Engine, Column, DateTime, GlobalSecondaryIndex,
                   LocalSecondaryIndex, Integer, String, UUID)
import arrow
import uuid
engine = Engine()


class IndexPost(engine.model):
    forum = Column(String, hash_key=True)
    user = Column(UUID, range_key=True)
    date = Column(DateTime)
    views = Column(Integer)

    by_user = GlobalSecondaryIndex(hash_key="user",
                                   projection="keys_only",
                                   write_units=1, read_units=10)

    by_date = LocalSecondaryIndex(range_key="date",
                                  projection=["views"])
engine.bind()


def main():
    uid = uuid.uuid4
    posts = 4
    user1 = uid()
    user2 = uid()
    user3 = uid()
    users = [user1, user2, user3, user1]
    forums = [
        "Support",
        "Announcements",
        "Support",
        "Announcements"
    ]

    dates = [
        arrow.now(),
        arrow.now().replace(hours=-2),
        arrow.now().replace(hours=-4),
        arrow.now().replace(hours=-6)
    ]

    posts = [IndexPost(forum=forum, user=user, date=date, views=0) for
             (forum, user, date) in zip(forums, users, dates)]
    engine.save(posts)

    print("Posts by user1.\nGSI projection includes (forum, user)\n")
    user_posts = engine.query(IndexPost.by_user)
    for post in user_posts.key(IndexPost.user == user1):
        print(post)
    print("\nPosts within the last 3 hours in forum 'Support'."
          "\nLSI projection contains (forum, user, date, views)\n")
    recent_posts = engine.query(IndexPost.by_date)
    forum_key = IndexPost.forum == "Support"
    date_key = IndexPost.date.between(arrow.now().replace(hours=-3),
                                      arrow.now())
    for post in recent_posts.key(forum_key & date_key):
        print(post)

if __name__ == "__main__":
    main()
