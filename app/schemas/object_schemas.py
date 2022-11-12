import uuid


class EventProvider:
    __tablename__ = "event_provider"

    uuid = Column(String(20), primary_key=True, default=lambda x: uuid.uuid4())
    provider_name = Column(String(20))


class EventConsumer:
    __tablename__ = "event_consumer"

    uuid = Column(String(20), primary_key=True, default=lambda x: uuid.uuid4())
    consumer_name = Column(String(20))

    account_rel_type = Column(String(20))
    account = Column(String(20))
