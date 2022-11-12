from sqlalchemy import Column, String
from sqlalchemy.orm import Session
from sqlalchemy import Column, String, DateTime
from sqlalchemy.sql import func

from ..fdatabase import Base
from enum import Enum


class Topic(Base):
    __tablename__ = "topic"

    uid = Column(String(20), primary_key=True)
    name = Column(String(20))
    provider_uid = Column(String(30))

    topic_state = Column(String(20))
    topic_type = Column(String(20))

    website = Column(String(200))

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class TopicType(Enum):
    rpg_game: str = "R"
    prob_game: str = "P"
    raffle: str = "F"
    lottery: str = "L"

class TopicState(Enum):
    registered: str = "R"
    banned: str = "B"
    pending: str = "P"
    operating: str = "O"

class EventType(str, Enum):
    ono: str = "O"
    prob: str = "P"
    select: str = "S"


class TopicService:

    @classmethod
    def add_new_topic(cls, session: Session, uid: str, service_form: dict):
        t = Topic(
            uid=uid,
            **service_form
        )
        session.add(t)
        session.flush()
        return t

class Event(Base):
    __tablename__ = "event"

    uid = Column(String(20), primary_key=True)
    topic_uid = Column(String(20), nullable=False)

    event_type = Column(String(8), nullable=False)
    event_sub_type = Column(String(8), nullable=True)
    event_payload = Column(String(200), nullable=True)

    provider_uid = Column(String(30), nullable=False)
    owner = Column(String(30), nullable=False)

    timestamp = Column(String(20))
    event_state = Column(String(20))

