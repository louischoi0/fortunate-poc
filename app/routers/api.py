from fastapi import APIRouter, Depends, Form
from pydantic.main import BaseModel
from pydantic import AnyUrl
from sqlalchemy.orm import Session
import passlib.hash as halg

from secrets import token_bytes
from base64 import b64encode
from uuid import uuid4

from ..dependencies import get_token_header
from ..authutils import get_current_user
from ..fdatabase import get_database
from ..schemas.user import User, ProviderService, ConsumerService
from ..httputils import response


router = APIRouter(
    prefix="/api",
    responses={404: {"description": "Not found"}},
)


class ProviderAuthMethodRegisterForm:
    pass

class ConsumerRegisterForm(BaseModel):
    local_id: str = ""
    local_password: str = ""
    topic_uid: str = ""

    @classmethod
    def as_form(
        cls,
        local_id: str = Form(...),
        local_password: str = Form(...),
        topic_uid: str = Form(...),
    ):
        return cls(local_id=local_id, local_password=local_password, topic_uid=topic_uid)

class ConsumerLoginForm(BaseModel):
    topic_uid: str 
    local_id: str 
    local_password: str 

    @classmethod
    def as_form(
        cls,
        topic_uid: str = Form(...),
        local_id: str = Form(...),
        local_password: str = Form(...),
    ):
        return cls(service_uid=topic_uid, local_id=local_id, local_password=local_password, topic_uid=topic_uid)

@router.post("/{provider_uid}/token/consumer")
def generate_consumer_token(
    provider_uid: str, 
    form: Form = Depends(ConsumerLoginForm.as_form),
    session: Session = Depends(get_database),
):
    c = ConsumerService.get_consumer(session, form.local_id, form.topic_uid)

    secret_key = ProviderService.get_provider_secret_key(session, provider_uid)
    seed = secret_key + form.local_password

    login_success = halg.pbkdf2_sha256.verify(seed, c.local_password)

    if login_success:
        return response(c.as_dict()) 
    else:
        return response({}, 201, "Invalid Consumer Auth Infomation.")

@router.post("/") 
def register_provider(
    session: Session = Depends(get_database),
    user: User = Depends(get_current_user)
): 
    secret_key = b64encode(token_bytes(32)).decode()
    ProviderService.register_provider(session, user, secret_key)
    session.flush()

    return {
        "secret_key": secret_key,
    }


@router.post("/consumer")
def register_consumer_to_provider(
    form: Form = Depends(ConsumerRegisterForm.as_form),
    session: Session = Depends(get_database),
    user: User = Depends(get_current_user)
):
    uid = user["uid"] 
    provider = ProviderService.get_provider(session, uid)
    provider = provider.as_dict()
    provider_uid = provider["uid"]

    seed = provider["secret_key"] + form.local_password
    hashed_local_pass = halg.pbkdf2_sha256.hash(seed)

    consumer = ConsumerService.register_consumer(
        session, 
        provider_uid=provider_uid,
        topic_uid=form.topic_uid,
        local_id=form.local_id,
        local_password_hashed=hashed_local_pass,     
    )
    consumer = consumer.as_dict() 
    assert halg.pbkdf2_sha256.verify(seed, hashed_local_pass)
    consumer["secret_key"] = provider["secret_key"]

    session.flush()
    return response(consumer)

