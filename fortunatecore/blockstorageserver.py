import socket
import threading
from utils import get_logger, get_timestamp, BufferCursor
from multiprocessing import Process
from time import sleep
from fortunate_system_const import *
from utils import padding_msg, padding_msg_front, verify_msg_optype, strpshift
from leveldb import LevelDB


class BlockStorageClient:
    logger = get_logger("BlockStorageClient")

    def __init__(self, client_id, *args, **kwargs):
        self._id = client_id
        self.sock = None
        self.api = None

    def init_client(self, *args, **kwargs):
        self.api = BlockStorageClientImpl(self)
        BlockStorageClient.logger.info("Connect to storage server.")
        self.api.connect()

    @classmethod
    def get_client(cls, client_id, *args, **kwargs):
        client = BlockStorageClient(client_id)
        client.init_client()
        return client


class BlockStorageClientImpl:
    def __init__(self, client, *args, **kwargs):
        self.client = client
        self.logger = client.logger

    def connect(self, *args, **kwargs):
        serveraddr = (
            BlockStorageServer.BLOCK_STORAGE_SERVER_HOST,
            BlockStorageServer.BLOCK_STORAGE_SERVER_PORT,
        )
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect(serveraddr)

        self.client.sock = client_sock

        return client_sock

    def _request_get_block_finalized(self, sign_key, *args, **kwargs):
        msg = "!gblcf"
        msg += sign_key
        msg = msg.encode('utf8')

        self.client.sock.send(msg)
        self.logger.debug(f"call:request_block_finalized - sign_key: {sign_key}")

        response = self.client.sock.recv(8192*12)
        
        assert response[:6].decode('utf8') == "@gblcf"

        return response

    def get_blockbuffer_finalized(self, sign_key, *args, **kwargs):
        msg = self._request_get_block_finalized(sign_key)
        
        cursor = BufferCursor(msg)
        _ = cursor.advance(OP_PREFIX_LEN)

        length = cursor.advance(BLOCK_RECORD_COUNT_FLAG_LEN).decode('utf8')
        length = int(length) 

        return length, cursor.rest().decode('utf8')

    def request_insert_block_row(self, sign_key, record_buffer, *args, **kwargs):
        if not type(record_buffer) == str:
            record_buffer = record_buffer.decode('utf8')

        requested_at = get_timestamp()

        msg = "!addrc"
        msg += str(requested_at)
        msg += sign_key
        msg += record_buffer
        msg = msg.encode("utf8")

        self.logger.debug(f"call:request_insert_block_row - data: {msg.decode('utf8')}")
        self.client.sock.send(msg)

        response_msg = self.client.sock.recv(8192*8)
        response_msg = response_msg.decode("utf8")

        op_type = response_msg[:6]
        assert op_type == "@addrc"

    def request_record_block(self, sign_key, *args, **kwargs):
        requested_at = get_timestamp()

        msg = "!rblck"
        msg += str(requested_at)
        msg += sign_key

        self.logger.info(f"call:request_request_block - sign_key: {sign_key}")

        msg = msg.encode("utf8")
        self.client.sock.send(msg)

        response_msg = self.client.sock.recv(8192*8)
        response_msg = response_msg.decode("utf8")

        op_type = response_msg[:6]
        assert op_type == "@rblck"

        block_buffer = response_msg[6:]
        self.logger.debug(f"get block buffer.")

    def request_commit_insert_block(self, sign_key, *args, **kwargs):
        requested_at = get_timestamp()

        msg = "!ciblc"
        msg += str(requested_at)
        msg += sign_key
        msg = msg.encode("utf8")

        self.logger.debug(f"call:request_commit_insert_block - sign_key: {sign_key}")
        self.client.sock.send(msg)

        response_msg = self.client.sock.recv(8192*8)
        verify_msg_optype(response_msg, "@ciblc")

    def request_commit_block(self, sign_key, *args, **kwargs):
        requested_at = get_timestamp()

        msg = "!cblck"
        msg += str(requested_at)
        msg += sign_key
        msg = msg.encode("utf8")

        self.logger.debug(f"call:request_commit_block - sign_key: {sign_key}")
        self.client.sock.send(msg)

        response_msg = self.client.sock.recv(8192*8)
        verify_msg_optype(response_msg, "@cblck")


class BlockStorageServer:
    BLOCK_STORAGE_SERVER_PORT = 5049
    BLOCK_STORAGE_SERVER_HOST = ""

    logger = get_logger("BlockStorageServer")

    def __init__(self, *args, **kwargs):
        self.blockmap = {}

        self.db = LevelDB("block")

    def app(self, *args, **kwargs):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as blockserver_sock:
            blockserver_sock.bind(
                (
                    BlockStorageServer.BLOCK_STORAGE_SERVER_HOST,
                    BlockStorageServer.BLOCK_STORAGE_SERVER_PORT,
                )
            )

            blockserver_sock.listen()
            BlockStorageServer.logger.info(
                f"open block storage server : {BlockStorageServer.BLOCK_STORAGE_SERVER_PORT}"
            )

            while True:
                blockclient_sock, client_addr = blockserver_sock.accept()

                th = threading.Thread(target=self.session, args=(blockclient_sock,))
                BlockStorageServer.logger.info("Fork thread for blockclient_sock")

                th.start()

    def session(self, blockclient_sock, *args, **kwargs):
        while True:
            msg = blockclient_sock.recv(8192*8)

            if not msg:
                self.logger.info("session is closed.")
                break

            self.logger.debug(f"blockserver received msg: {msg}")

            self.call(blockclient_sock, msg)

    def call(self, sock, request_msg):
        op_type = request_msg.decode("utf8")[:6]

        msg = request_msg[6:]

        if op_type == "!addrc":
            self.reply_insert_block_row(sock, msg)

        elif op_type == "!rblck":
            self.reply_get_block(sock, msg)

        elif op_type == "!cblck":
            self.reply_commit_block(sock, msg)
        
        elif op_type == "!gblcf":
            self.reply_get_block_finalized(sock, msg)

    @classmethod
    def get_blockserver_sock(cls):
        blockserver_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        addr = (cls.BLOCK_STORAGE_SERVER_HOST, cls.BLOCK_STORAGE_SERVER_PORT)
        blockserver_sock.connect(addr)

        return blockserver_sock

    def reply_commit_insert_block(self, sock, msg, *args, **kwargs):
        c = BufferCursor(msg)

        ts = c.advance(TIMESTAMP_STR_LEN).decode("utf8")
        sign_key = c.advance(POOL_SIGN_KEY_LEN).decode("utf8")

        record_block = self.blockmap[sign_key]
        block_buffer = BlockStorageImpl.serialize_block((sign_key, record_block))
        
        old_data = self.db.Get(sign_key.encode('utf8'))
        block_buffer = old_data + block_buffer

        self.db.Put(sign_key.encode("utf8"), block_buffer)
        del self.blockmap[sign_key] # flush block in memory.

        response = "@ciblc"
        response = response.encode("utf8")

        sock.send(response)

    def reply_commit_block(self, sock, msg, *args, **kwargs): # overwrite
        c = BufferCursor(msg)

        ts = c.advance(TIMESTAMP_STR_LEN).decode("utf8")
        sign_key = c.advance(POOL_SIGN_KEY_LEN).decode("utf8")

        record_block = self.blockmap[sign_key]
        block_buffer = BlockStorageImpl.serialize_block((sign_key, record_block))

        self.db.Put(sign_key.encode("utf8"), block_buffer)
        del self.blockmap[sign_key] # flush block in memory.

        response = "@cblck"
        response = response.encode("utf8")

        sock.send(response)

    def reply_insert_block_row(self, sock, msg, *args, **kwargs):
        c = BufferCursor(msg)
        self.logger.debug(f"reply_insert_block_row - msg: {msg}")

        ts = c.advance(TIMESTAMP_STR_LEN).decode("utf8")
        sign_key = c.advance(POOL_SIGN_KEY_LEN).decode("utf8")


        record = c.advance(256).decode("utf8")

        self.logger.debug(
            f"call: reply_insert_block_row - sign_key: {sign_key}, record_type: {record[:2]}, record: {record[2:]}"
        )

        while record and len(record) == 256:
    
            if sign_key in self.blockmap:
                self.blockmap[sign_key].append(record)
            else:
                self.blockmap[sign_key] = [record]

            record = c.advance(256).decode("utf8")

        response = "@addrc".encode("utf8")
        self.logger.debug(
            f"okay: reply_insert_block_row - sign_key: {sign_key}, record: {record}, response: {response}"
        )
        sock.send(response)

    def reply_get_block(self, sock, msg, *args, **kwargs):
        self.logger.info(f"call: reply_get_block - msg: {msg}")
        c = BufferCursor(msg)

        ts = c.advance(TIMESTAMP_STR_LEN).decode("utf8")
        sign_key = c.advance(POOL_SIGN_KEY_LEN).decode("utf8")

        record_block = self.blockmap[sign_key]
        block_buffer = BlockStorageImpl.serialize_block((sign_key, record_block))

        response = "@rblck".encode("utf8")
        response += block_buffer

        sock.send(response)

    def reply_get_block_finalized(self, sock, msg, *args, **kwargs):
        self.logger.info(f"call: reply_get_block_finalized from msg: {msg}")

        c = BufferCursor(msg)
        sign_key = c.advance(POOL_SIGN_KEY_LEN)
        
        self.logger.info(f"get block from database - sign key: {sign_key}")
        block_buffer = self.db.Get(sign_key)
        block_length = str(int(len(block_buffer) / 256))
    
        response = "@gblcf"
        response += padding_msg_front(block_length, BLOCK_RECORD_COUNT_FLAG_LEN)
        response = response.encode('utf8')
        response += block_buffer

        sock.send(response)

class BlockStorageImpl:
    @classmethod
    def serialize_block(self, block_key_pair):
        sign_key, record_block = block_key_pair
        serialized_at = get_timestamp()
        
        buffer = ""

        """
        buffer += str(serialized_at)
        buffer += sign_key
        counts = len(record_block)

        sig_count_field = padding_msg_front(str(counts), SIGNAL_COUNT_FIELD)
        buffer += sig_count_field
        """

        for record in record_block:
            record = padding_msg(record, BLOCK_RECORD_LEN)
            print(f'bss: len: {len(record)},  {record}')
            buffer += record

        return buffer.encode("utf8")

    def signal_to_buffer(self, sign_key, signal):
        pass

    def event_to_buffer(self, sign_key, event):
        pass


## TEST ##
def _runserver():
    bss = BlockStorageServer()
    bss.app()


def _runclient():
    bc = BlockStorageClient("client0")
    bc.init_client()

    sign_key = "abcdefgh"
    bc.api.request_insert_block_row(sign_key, "123456")
    bc.api.request_record_block(sign_key)
    bc.api.request_commit_block(sign_key)


def _test():
    server_proc = Process(target=_runserver)
    server_proc.start()

    from time import sleep

    sleep(1.2)
    _runclient()

    server_proc.join()


## ##
if __name__ == "__main__":
    #_test()
    _runserver()

