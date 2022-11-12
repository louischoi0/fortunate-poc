from fastapi import APIRouter, Depends
from ..dependencies import get_token_header
from ..authutils import get_current_user
from ..schemas.user import User
from ..schemas.event import EventType

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


@router.post("/{topic}/{consumer}")
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
