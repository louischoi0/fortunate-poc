from collections import namedtuple

from fastapi import Depends

from sqlalchemy import Column, String, DateTime
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import and_

from uuid import uuid4
from ..fdatabase import Base, get_database

UserTuple = namedtuple("User", ["uid", "email", "name", "user_type", "password"])


class User(Base):
    __tablename__ = "user"

    uid = Column(String(20), primary_key=True)
    email = Column(String(50))
    name = Column(String(20))
    user_type = Column(String(4))
    password = Column(String(100))

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def as_dict(self):
        return {
            "uid": self.uid,
            "email": self.email,
            "name": self.name,
            "user_type": self.user_type,
            "password": self.password,
        }

    def as_tuple(self):
        return UserTuple(**self.as_dict())


class UserService:
    @classmethod
    def get_user_by_email(cls, session: Session, email: str):
        return session.query(User).filter(User.email == email).first()

    @classmethod
    def add_new_user(cls, session: Session, user_form: dict):
        user = User(
            uid=uuid4().hex[:20],
            email=user_form.email,
            password=user_form.password,
            name=user_form.name,
            user_type=user_form.user_type,
        )
        session.add(user)
        session.flush()


class Provider(Base):
    __tablename__ = "provider"

    uid = Column(String(20), primary_key=True)
    secret_key = Column(String(200))

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def as_dict(self):
        return {
            "uid": self.uid,
            "secret_key": self.secret_key
        }

class ProviderService:

    @classmethod
    def register_provider(cls, session: Session, user: User, provider_secret_key: str):
        p = Provider(
            uid=user["uid"],
            secret_key=provider_secret_key,
        )

        session.add(p)
        return p
    
    @classmethod
    def get_provider(
        cls, 
        session: Session,
        uid: str,
    ):
        return session.query(Provider).filter(Provider.uid == uid).first()

    @classmethod 
    def get_provider_secret_key(
        cls, 
        session: Session,
        uid: str
    ):
        return session.query(Provider).filter(Provider.uid == uid).first().secret_key

class Consumer(Base):
    __tablename__ = "consumer"

    uid = Column(String(20), primary_key=True)

    provider_uid = Column(String(50), nullable=False)
    topic_uid = Column(String(50), nullable=False)

    local_id = Column(String(50), nullable=False)
    local_password = Column(String(200), nullable=False)

    def as_dict(self):
        return {
            "uid": self.uid,
            "provider_uid": self.provider_uid,
            "service_uid": self.topic_uid,
            "local_id": self.local_id,
            "local_password": self.local_password
        }

class ConsumerService:

    @classmethod
    def generate_consumer_uid(cls):
        return uuid4().hex[:16]

    @classmethod
    def get_consumer(cls, session, local_id: str, topic_uid: str):
        return session.query(Consumer).filter(and_(Consumer.topic_uid == topic_uid, Consumer.local_id == local_id)).first()

    @classmethod
    def register_consumer(
        cls,
        session: Session,
        provider_uid: str,
        topic_uid: str,
        local_id: str,
        local_password_hashed: str 
    ):
        c = Consumer(
            uid=cls.generate_consumer_uid(),
            provider_uid=provider_uid,
            topic_uid=topic_uid,
            local_id=local_id,
            local_password=local_password_hashed
        )

        session.add(c)
        return c