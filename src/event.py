from pool import generate_number_sequence
from secrets import token_hex
from fortune_system_const import *
from utils import get_timestamp

class ForunateEvent:
    def __init__(self, event_type, event_secondary_type = None, buffer =b'',*args, **kwargs):
        self.created_at = get_timestamp()

        self.event_primary_type = event_type
        self.event_secondary_type = event_secondary_type

        self.buffer = buffer
    
    def serialize(self, *args, **kwargs):
        pass

class FortunateServerImpl:

    def __init__(self, pool, *args, **kwargs):
        self.pool = pool



    def init_server_impl(self, *args, **kwargs):
        pass
    
    def get_event_hash(self, *args, **kwargs):
        return token_hex(EVENT_HASH_LEN)

    def create_event(self, *args, **kwargs):
        pass

    



class FortunateSever:
    def __init__(self, *args, **kwargs):
        pass


