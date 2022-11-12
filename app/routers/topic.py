from fastapi import APIRouter, Depends
from pydantic.main import BaseModel
from pydantic import AnyUrl

from uuid import uuid4

from ..dependencies import get_token_header
from .auth import *
from ..authutils import get_current_user
from ..schemas.event import TopicType, TopicService

router = APIRouter(
    prefix="/topic",
    responses={404: {"description": "Not found"}},
)

def generate_endpoint():
    return uuid4().hex[:16]

class TopicRegisterForm(BaseModel):
    name: str
    website: AnyUrl
    topic_type: TopicType

    @classmethod
    def as_form(cls,
        name: str = Form(...),
        topic_type: str = Form(...),
        website: AnyUrl = Form(...)
    ):
        return cls(name=name, website=website, topic_type=topic_type)
    
    def as_dict(
        self
    ): 
        return {
            "name": self.name,
            "website": self.website,
            "topic_type": self.topic_type
        }


@router.post('/')
def register_service(
    current_user: Form = Depends(get_current_user),
    register_form: TopicRegisterForm = Depends(TopicRegisterForm.as_form),
    session: Session = Depends(get_database)
):

    uid = generate_endpoint()
    form = register_form.as_dict()
    form["provider"] = current_user["uid"]
    form["topic_type"] = form["topic_type"].value

    form["topic_state"] = "R"

    return TopicService.add_new_topic(session, uid, form)