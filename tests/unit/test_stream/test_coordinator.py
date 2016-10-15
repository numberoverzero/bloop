from bloop.stream.coordinator import Coordinator


def test_coordinator_repr(engine, session):
    stream_arn = "stream-arn"
    coordinator = Coordinator(engine=engine, session=session, stream_arn=stream_arn)
    assert repr(coordinator) == "<Coordinator[stream-arn]>"
