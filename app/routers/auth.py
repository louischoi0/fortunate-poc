from fastapi import APIRouter, Depends, Form
from enum import Enum

from pydantic.main import BaseModel
from pydantic.networks import EmailStr
from sqlalchemy.orm import Session

from ..dependencies import get_token_header
from ..fdatabase import get_database
from ..schemas.user import UserService
from ..httputils import response
from ..authutils import create_access_token, get_hashed_password, get_current_user

router = APIRouter(
    prefix="/auth",
    responses={404: {"description": "Not found"}},
)

class UserType(str, Enum):
    provider: str = "P"
    consumer: str = "C"


class UserSignUpForm(BaseModel):
    email: EmailStr
    name: str = ""
    password: str = ""
    user_type: str = ""

    @classmethod
    def as_form(
        cls,
        email: str = Form(...),
        name: str = Form(...),
        password: str = Form(...),
        user_type: UserType = Form(...),
    ):
        return cls(email=email, name=name, password=password, user_type=user_type)

    def as_dict(self):
        return {
            "email": self.email,
            "name": self.name,
            "password": self.password,
            "user_type": self.user_type,
        }


@router.post("/signup/")
async def signup(
    user_form: Form = Depends(UserSignUpForm.as_form),
    session: Session = Depends(get_database),
):
    email = user_form.email
    password = get_hashed_password(user_form.password)
    user_form.password = password

    if UserService.get_user_by_email(session, email):
        return response({}, 201, "Email Already exists.")

    UserService.add_new_user(session, user_form=user_form)
    token = create_access_token(user_form.as_dict())

    return response({**user_form.as_dict(), "token": token})





