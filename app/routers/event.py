import uuid
from fastapi import APIRouter, Depends, Form
from ..dependencies import get_token_header
from ..authutils import get_current_user
from ..schemas.user import User
from ..schemas.event import EventType, EventService

from fastapi import APIRouter, Depends
from pydantic.main import BaseModel
from pydantic import AnyUrl

from sqlalchemy.orm import Session

from ..fdatabase import Base, get_database

router = APIRouter(
    prefix="/event",
    responses={404: {"description": "Not found"}},
)

class EmitEventForm:
    event_type: EventType 
    event_sub_type: str
    event_payload: str

    @classmethod
    def as_form(
        cls,
        event_sub_type: str,
        event_payload: str
    ): 
        return 1


@router.post("/deprecated/{topic}/{consumer}")
async def emit_event(
    topic: str,
    consumer: str,
    provider: User = Depends(get_current_user)    
):
    d = {
        "topic": topic,
        "consumer": consumer,
        "provider": provider,
    }

    return d

class EventApplicationForm(BaseModel):
    event_uuid: int

    user_name: str
    phone_number: str
    email: str

    content: str

    @classmethod
    def as_form(cls,
        event_uuid: int = Form(...),
        user_name: str = Form(...),
        phone_number: str = Form(...),
        email: str = Form(...),
        content: str = Form(...)
    ):
        return cls(
            event_uuid=event_uuid,
            user_name=user_name,
            phone_number=phone_number,
            email=email,
            content=content
        )

    def as_dict(self):
        return {
            "event_uuid": self.event_uuid,
            "user_name": self.user_name,
            "phone_number": self.phone_number,
            "email": self.email,
            "content": self.content
        }

class EventRegisterForm(BaseModel):
    user_name: str
    user_email: str
    event_title: str
    event_description: str

    event_type: str

    prop1: str
    prop2: str
    prop3: str 

    outlink: str
    duedate: str

    @classmethod
    def as_form(cls,
        user_name: str = Form(...),
        user_email: str = Form(...),
        event_title: str = Form(...),
        event_description: str = Form(...),
        event_type: str = Form(...),
        prop1: str = Form(...),
        prop2: str = Form(...),
        prop3: str = Form(...),
        outlink: str = Form(...),
        duedate: str = Form(...),
    ):
        return cls(
            user_name=user_name,
            user_email=user_email,
            event_title=event_title,
            event_description=event_description,
            event_type=event_type,
            prop1=prop1,
            prop2=prop2,
            prop3=prop3,
            outlink=outlink,
            duedate=duedate,
        )

    def as_dict(
        self
    ): 
        return {
            "user_name": self.user_name,
            "user_email": self.user_email,
            "event_title": self.event_title,
            "event_description": self.event_description,
            "event_type": self.event_type,
            "prop1": self.prop1,
            "prop2": self.prop2,
            "prop3": self.prop3,
            "outlink": self.outlink,
            "duedate": self.duedate,
        }

@router.get("/{event_token}")
def get_event(
    event_token: str,
    session: Session = Depends(get_database),
): 
    return EventService.get(session, event_token)

@router.post("/")
async def emit_event(
    register_form: EventRegisterForm = Depends(EventRegisterForm.as_form),
    session: Session = Depends(get_database),
):
    d = register_form.as_dict()
    d["token"] = uuid.uuid4().hex
    EventService.add(session, d)

    return d

@router.post("/apply")
def apply_event(
    form: Form = Depends(EventApplicationForm.as_form),
    session: Session = Depends(get_database),
):
    a = form.as_dict()
    EventService.apply(session, a)
    return a

