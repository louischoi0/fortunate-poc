from typing import Union, List, Optional
from pydantic import BaseModel

from fastapi import FastAPI, Query, Form
from fastapi import Depends, HTTPException, status

from starlette.middleware.cors import CORSMiddleware
from starlette.middleware import Middleware

from sqlalchemy.orm import Session
from uuid import UUID

from .routers import api, auth, event, topic
from .schemas.event import Event
from .fdatabase import get_database, Base, engine
from .schemas.user import User, UserService
from .authutils import (
    get_hashed_password,
    verify_password,
    create_access_token,
    SECRET_KEY,
    ALGORITHM,
)
from .httputils import response
from .authutils import oauth2_schme, get_current_user

middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=[
            "*",
            "http://localhost:3000",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
]


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    uid: str
    username: str
    user_type: str


class UserLoginFrom(BaseModel):
    email: str
    password: str

    @classmethod
    def as_form(
        cls,
        email: str = Form(...),
        password: str = Form(...),
    ):
        return cls(email=email, password=password)


app = FastAPI(middleware=middleware)

app.include_router(auth.router)
app.include_router(event.router)
app.include_router(topic.router)
app.include_router(api.router)

Base.metadata.create_all(bind=engine)


def get_user_dict():
    return {
        "uid": "a2f1c921",
        "name": "louis",
        "password": "1234",
        "user_type": "P",
    }


@app.post("/signup")
async def signup(form_data):
    pass


@app.post("/token")
async def login(
    form_data: Form = Depends(UserLoginFrom.as_form),
    session: Session = Depends(get_database),
):

    email = form_data.email
    user = UserService.get_user_by_email(session, email)

    if not user:
        return response({}, 201, "Email does not exists.")

    user_dict = user.as_dict()
    origin_hashed_pw = user_dict["password"]

    if not verify_password(form_data.password, origin_hashed_pw):
        return HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid autentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    else:
        access_token = create_access_token(data=user_dict)

        return {
            "access_token": access_token,
            "token_type": "bearer",
        }


@app.get("/items/")
async def read_items(token: str = Depends(oauth2_schme)):
    results = {"items": [{"item_id": "Foo"}, {"item_id": "Bar"}]}
    if q:
        results.update({"q": q})
    return results


class EventResponseDTO(BaseModel):
    event_hash = str
    event_type = str
    timestamp = str
    event_state = str

    class Config:
        orm_mode = True



@app.post("/users/me")
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user
