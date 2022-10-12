from pool import Pool, PoolBackend
from secrets import token_hex
from fortunate_system_const import *
from utils import get_timestamp, get_logger, padding_msg
import hashlib
from blockstorageserver import BlockStorageClient, BlockStorageServer
from time import sleep
from multiprocessing import Process
from node import NodeApiImpl


class FortunateEvent:
    def __init__(
        self,
        event_hash,
        event_type,
        event_secondary_type=None,
        sign_key="",
        requested_at=0,
        buffer=b"",
        *args,
        **kwargs
    ):

        self.event_hash = event_hash
        self.created_at = get_timestamp()

        self.event_primary_type = event_type
        self.event_secondary_type = event_secondary_type

        self.sign_key = sign_key

        self.buffer = buffer
        self.requested_at = requested_at

    def serialize_header(self, *args, **kwargs):
        buffer = str(self.created_at)
        buffer += self.sign_key
        buffer += self.event_hash
        buffer += str(get_timestamp())
        buffer += "02"

        self.buffer = buffer
        return self.buffer
    
    def attach_event_payload(self, payload, *args, **kwargs):
        self.buffer += payload
        return self.buffer


    @classmethod
    def get_event_hash(self, *args, **kwargs):
        seed = time.time()
        hasher = hashlib.sha256()
        hasher.update(str(seed).encode("utf8"))
        return hasher.hexdigest()[:EVENT_HASH_LEN]


class FortunateServerImpl:
    def __init__(self, pool, *args, **kwargs):
        self.pool = pool

    def init_server_impl(self, *args, **kwargs):
        pass

    def get_event_hash(self, *args, **kwargs):
        return token_hex(EVENT_HASH_LEN)

    def create_event(self, *args, **kwargs):
        pass

    def serialize_event_buffer(self, event, signals, *args, **kwargs):
        header = event.serialize_header()
        payload = ""

        for signal in signals:
            payload += NodeApiImpl.parse_signal_id_from_buffer(signal)

        return event.attach_event_payload(payload) 
    

class FortunateServer:

    logger = get_logger("FortunateServer")

    def __init__(self, *args, **kwargs):
        self.blockstorage_client = None
        self.pool = None
        self.pool_backend = None
        self.impl = None

        self.session = {}
    
    def connect_block_server(self):
        self.blockstorage_client = BlockStorageClient.get_client("fortunate_server")

    def init_server(self, *args, **kwargs):
        self.connect_block_server()

        p = Pool()
        p.init_pool()
        backend = PoolBackend(p)
        backend.init_backend()

        self.pool = p
        self.pool_backend = backend

        self.impl = FortunateServerImpl(self.pool)
        return backend.ready()

    def api(self, event_request, *args, **kwargs):
        sign_key = self.pool.sign_key
        
        event_type = event_request["event_type"]
        event_secondary_type = event_request["event_secondary_type"]
        event_hash = event_request["event_hash"]
        requested_at = get_timestamp()
        
        self.logger.info(f"create event: event_hash: {event_hash}, event_type: {event_type}, signkey: {sign_key}, ")
        event = FortunateEvent(event_hash, event_type, event_secondary_type, sign_key)
        sign_key = self.pool.sign_key

        if event_type == "tf" and event_secondary_type == "1;4":
            signals = self.pool.backend.get_signals_from_node_pool(event_hash, 2)
            response = self.impl.serialize_event_buffer(event, signals)
            response = padding_msg(response, BLOCK_RECORD_LEN)
            self.blockstorage_client.api.request_insert_block_row(sign_key, response.encode('utf8'))
        
        self.logger.info(f"api response: {response}")
        return response


    def trigger_event(self, event_type, event_secondary_type, *args, **kwargs):
        sign_key = self.pool.sign_key
        e = FortunateEvent(event_type, event_secondary_type, sign_key)

        return e

    def app(self, *args, **kwargs):

        while True:
            from time import sleep
            sleep(5)



def _runblockstorageserver():
    bss = BlockStorageServer()
    bss.app()

def _runnode(port):
    import node
    _, _, nodebackend = node.create_node(port)
    nodebackend.open()

if __name__ == "__main__":
    blockserverproc = Process(target=_runblockstorageserver)
    blockserverproc.start()
    sleep(1.5)

    p0 = Process(target=_runnode, args=(5050,))
    p0.start()

    p1 = Process(target=_runnode, args=(5051,))
    p1.start()

    sleep(1.5)

    server = FortunateServer()
    poolproc = server.init_server()

    server.connect_block_server()

    event_request = {
        "event_type": "tf",
        "event_secondary_type": "1;4",
        "event_hash": "thisisev",
    }

    server.api(event_request)
    server.blockstorage_client.api.request_commit_block(server.pool.sign_key)
    sleep(2)

    from sys import exit
    p0.terminate()
    p1.terminate()
    blockserverproc.terminate()
    exit(0)
