import arrow
from bloop import (Engine, Column, Integer, DateTime,
                   GlobalSecondaryIndex, String)
engine = Engine()


class User(engine.model):
    class Meta:
        write_units = 10

    id = Column(Integer, hash_key=True)
    name = Column(String)
    email = Column(String)

    by_email = GlobalSecondaryIndex(hash_key='email',
                                    projection='keys_only',
                                    write_units=1, read_units=5)


class Tweet(engine.model):
    user = Column(Integer, hash_key=True)
    id = Column(String, range_key=True)
    content = Column(String)
    date = Column(DateTime)

engine.bind()

user = User(id=100, name='@garybernhardt', email='foo@bar.com')
tweet = Tweet(
    user=user.id, id='600783770925420546', date=arrow.now(),
    content=(
        'Consulting service: you bring your big data problems'
        ' to me, I say "your data set fits in RAM", you pay me'
        ' $10,000 for saving you $500,000.'))
engine.save([user, tweet])


email = 'foo@bar.com'
yesterday = arrow.now().replace(days=-1)

user = engine.query(User.by_email).key(User.email == email).first()
tweets = engine.query(Tweet).key(Tweet.user == user.id)

for tweet in tweets.filter(Tweet.date >= yesterday):
    print(tweet.content)
