import socket
import threading
from utils import get_logger, get_timestamp, BufferCursor
from multiprocessing import Process
from time import sleep
from fortunate_system_const import *
from utils import padding_msg, verify_msg_optype
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

    def request_insert_block_row(self, sign_key, record_buffer, *args, **kwargs):
        requested_at = get_timestamp()

        msg = "!addrc"
        msg += str(requested_at)
        msg += sign_key
        msg += record_buffer.decode('utf8')
        msg = padding_msg(msg, 128)

        msg = msg.encode("utf8")

        self.logger.info(f"call:request_insert_block_row - data: {msg.decode('utf8')}")
        self.client.sock.send(msg)

        response_msg = self.client.sock.recv(1024)
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

        response_msg = self.client.sock.recv(1024)
        response_msg = response_msg.decode("utf8")

        op_type = response_msg[:6]
        assert op_type == "@rblck"

        block_buffer = response_msg[6:]
        self.logger.info(f"get block buffer - buffer: {block_buffer}")

    def request_commit_block(self, sign_key, *args, **kwargs):
        requested_at = get_timestamp()

        msg = "!cblck"
        msg += str(requested_at)
        msg += sign_key
        msg = msg.encode("utf8")

        self.logger.info(f"call:request_commit_block - sign_key: {sign_key}")
        self.client.sock.send(msg)

        response_msg = self.client.sock.recv(1024)
        verify_msg_optype(reponse_msg)


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
            msg = blockclient_sock.recv(1024)
            self.logger.info(f"blockserver received msg: {msg}")

            self.call(blockclient_sock, msg)
            sleep(1)

    def call(self, sock, request_msg):
        op_type = request_msg.decode("utf8")[:6]

        msg = request_msg[6:]

        if op_type == "!addrc":
            self.reply_insert_block_row(sock, msg)

        elif op_type == "!rblck":
            self.reply_get_block(sock, msg)
        
        elif op_type == "!cblck":
            self.reply_commit_block(sock, msg)

    @classmethod
    def get_blockserver_sock(cls):
        blockserver_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        addr = (cls.BLOCK_STORAGE_SERVER_HOST, cls.BLOCK_STORAGE_SERVER_PORT)
        blockserver_sock.connect(addr)

        return blockserver_sock

    def reply_commit_block(self, sock, msg, *args, **kwargs):
        c = BufferCursor(msg)

        ts = c.advance(TIMESTAMP_STR_LEN).decode("utf8")
        sign_key = c.advance(POOL_SIGN_KEY_LEN).decode("utf8")
        
        record_block = self.blockmap[sign_key]
        block_buffer = BlockStorageImpl.serialize_block((sign_key, record_block))
        block_buffer = block_buffer.encode('utf8')

        self.db.Put(sign_key, block_buffer)

        response = "@rblck"
        response = response.encode('utf8')

        sock.reply(response)
        

    def reply_insert_block_row(self, sock, msg, *args, **kwargs):
        c = BufferCursor(msg)

        ts = c.advance(TIMESTAMP_STR_LEN).decode("utf8")
        sign_key = c.advance(POOL_SIGN_KEY_LEN).decode("utf8")
        record = c.advance(256)

        self.logger.info(
            f"call: reply_insert_block_row - sign_key: {sign_key}, record: {record}"
        )

        if sign_key in self.blockmap:
            self.blockmap[sign_key].append(record)
        else:
            self.blockmap[sign_key] = [record]

        response = "@addrc".encode("utf8")
        self.logger.info(
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


class BlockStorageImpl:

    @classmethod
    def serialize_block(self, block_key_pair):
        sign_key, record_block = block_key_pair
        serialized_at = get_timestamp()

        buffer = b""
        buffer += str(serialized_at).encode("utf8")
        buffer += sign_key.encode("utf8")

        sig_count_field = "0000"  # TODO

        for record in record_block:
            buffer += record

        return buffer

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
    _test()
    
