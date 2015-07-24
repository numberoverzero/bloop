import uuid
from models import engine, Account, Tweet


account = Account(
    id=uuid.uuid4(), name='@garybernhardt',
    email='REDACTED')
# This save will raise if the account id is already taken
unique_id = Account.id.is_(None)
engine.save(account, condition=unique_id)

account.name = 'garybernhardt'
del account.email
engine.save(account)

tweet = Tweet(
    account=account.id, id='616102582239399936',
    content='today, I wrote a type validator in Python, as you do',
    favorites=9)

engine.save([account, tweet])
engine.delete(account)

# Don't delete the tweet if it's been
# favorited enough for people to notice
not_popular = Tweet.favorites < 10
engine.delete(tweet, condition=not_popular)

same_account = Tweet.account == tweet.account
same_id = Tweet.id == tweet.id
same_content = Tweet.content == tweet.content

same_tweet = same_account & same_id & same_content
engine.save(tweet, condition=same_tweet)

with engine.context(atomic=True) as atomic:
    atomic.save(tweet)

engine.config['atomic'] = True
