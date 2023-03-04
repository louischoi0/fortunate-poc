from sqlalchemy import Column, String
from sqlalchemy.orm import Session
from sqlalchemy import Column, String, DateTime, Integer
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

    uuid = Column(Integer, primary_key=True)

    user_name = Column(String(50), nullable=False) 
    user_email = Column(String(200), nullable=False)
    token = Column(String(50), nullable=False)

    event_title = Column(String(100), nullable=False)
    event_description = Column(String(200), nullable=True)

    event_type = Column(String(8), nullable=False)
    event_sub_type = Column(String(8), nullable=True)
    event_payload = Column(String(200), nullable=True)

    prop1 = Column(String(50), nullable=True)
    prop2 = Column(String(50), nullable=True)
    prop3 = Column(String(50), nullable=True)

    provider_uid = Column(String(30), nullable=True)
    outlink = Column(String(500), nullable=True)

    duedate = Column(String(50), nullable=False)

    timestamp = Column(String(20))
    event_state = Column(String(20))

class EventApplication(Base):

    __tablename__ = "event_application"
    uuid = Column(Integer, primary_key=True)

    event_uuid = Column(Integer, nullable=False)

    user_name = Column(String(50), nullable=False)
    phone_number = Column(String(50), nullable=False)
    email = Column(String(50), nullable=False)

    content = Column(String(50), nullable=True)
    
    app_status = Column(String(50), nullable=True)


class EventService:

    @classmethod
    def get(cls, session: Session, token: str):
        return session.query(Event).filter(Event.token==token).one()


    @classmethod
    def add(cls, session: Session, event: dict):
        t = Event(**event)
        session.add(t)
        session.flush()
        return t

    @classmethod
    def apply(cls, session: Session, application: dict):
        a = EventApplication(**application)
        session.add(a)
        session.flush()
        return a