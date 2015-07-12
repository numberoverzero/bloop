"""
Combined source from the README's "Engine.model and sessions" section.
"""
from bloop import Engine, Column, UUID
import boto3.session
import uuid
regional_engines = {}


def engine(region):
    '''
    Ensure a single engine per region.

    Multiple calls with the same region return the same engine.
    '''
    engine = regional_engines.get(region)
    if not engine:
        session = boto3.session.Session(region_name=region)
        regional_engines[region] = engine = Engine(session=session)
    return engine


class EastModel(engine('us-east-1').model):
    id = Column(UUID, hash_key=True)


class WestModel(engine('us-west-2').model):
    id = Column(UUID, hash_key=True)

engine('us-east-1').bind()
engine('us-west-2').bind()


def main():
    uid = uuid.uuid4

    for region, model in [('us-east-1', EastModel), ('us-west-2', WestModel)]:
        instance = model(id=uid())
        engine(region).save(instance)
        print("Saved instance {} with engine({})".format(instance, region))

if __name__ == "__main__":
    main()
