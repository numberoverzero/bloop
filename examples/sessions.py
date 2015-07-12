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


class EastModel(engine('us-east-1')):
    id = Column(UUID, hash_key=True)


class WestModel(engine('us-west-2')):
    id = Column(UUID, hash_key=True)

engine('us-east-1').bind()
engine('us-west-2').bind()


def main():
    uid = uuid.uuid4
    east_model = EastModel(id=uid())
    west_model = WestModel(id=uid())

    engine('us-east-1').save(east_model)
    engine('us-west-2').save(west_model)

if __name__ == "__main__":
    main()
