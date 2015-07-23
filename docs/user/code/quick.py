from bloop import (Engine, Column, Integer,
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
engine.bind()

from bloop import ConstraintViolation

user = User(id=100, name='numberoverzero', email='foo@bar.com')
unique_id = User.id.is_(None)
try:
    engine.save(user, condition=unique_id)
except ConstraintViolation:
    print("User with id 100 already exists!")

user.name = 'new_name'
del user.email
engine.save(user)


class Tweet(engine.model):
    user = Column(Integer, hash_key=True)
    id = Column(String, range_key=True)
    content = Column(String)
engine.bind()

user = User(id=5, name='@numberoverzero', email='<REDACTED>')
tweet = Tweet(user=user.id, id='twitter url',
              content='Secret NSA Documents')

engine.save([user, tweet])
engine.delete([user, tweet])

# Save the tweet again so we can modify it
engine.save(tweet)

tweet.content = 'REDACTED'

# Don't redact the content unless it's still the secret documents
same_content = Tweet.content == 'Secret NSA Documents'
# Actually, if it still has the work 'Secret' let's redact it.
contains_secrets = Tweet.content.contains('Secret')

engine.save(tweet, condition=(same_content | contains_secrets))


same_user = Tweet.user == tweet.user
same_id = Tweet.id == tweet.id
same_content = Tweet.content == tweet.content

same_tweet = same_user & same_id & same_content
engine.save(tweet, condition=same_tweet)

with engine.context(atomic=True) as atomic:
    atomic.save(tweet)

engine.config['atomic'] = True
