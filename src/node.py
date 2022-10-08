from random import randint, choice
from functools import reduce
import hashlib
import socket
import leveldb
import time
import logging
from utils import get_logger, padding_msg, get_timestamp
from time import sleep
from fortunate_system_const import *
from blockstorageserver import BlockStorageClient

from utils import BufferCursor

FLAG_NAMES = ["1/2", "1/4", "1/8", "1/10", "1/100", "1/3"]
FLAG_POSITION = {"1/2": 0, "1/4": 1, "1/8": 2, "1/10": 3, "1/100": 4, "1/3": 5}

FLAG_THRES = {
    "1/2": 5000.0,
    "1/4": 2500.0,
    "1/5": 2000.0,
    "1/8": 1250.0,
    "1/10": 1000.0,
    "1/100": 100.0,
    "1/3": 3333.3333,
}


def fp(n):
    _n = randint(0, 10000)
    thres = FLAG_THRES[n]
    return _n < thres


class NodeBlockCommitter:
    def __init__(self):
        pass

    def commit(self, sign_key, block_buffer):
        pass


class NodeUUIDGenerator:
    def __init__(self):
        pass

    @classmethod
    def getid(cls):
        seed = time.time()
        hasher = hashlib.sha256()
        hasher.update(str(seed).encode("utf8"))
        return hasher.hexdigest()[:NODE_ID_LEN]

    @classmethod
    def getsigid(cls):
        seed = time.time()
        hasher = hashlib.sha256()
        hasher.update(str(seed).encode("utf8"))
        return hasher.hexdigest()[:NODE_SIGNAL_ID_LEN]


class NodeBackend:
    NODE_MAX_PACKET_SIZE = 1024
    NODE_PACKET_SIZE = 128
    logger = get_logger("FNodeBackend")

    def __init__(self, node, port, max_packet_size=1024):
        self.node = node
        self.port = port

        self.emitted = False

        self.sock = None
        self.client_sock = None

        self.api = None
        self.poolconnectionmap = {}

        self.blockstorage_client = None

    def init_node_backend(self):
        self.api = NodeApiImpl(self.node, self)
        self.blockstorage_client = BlockStorageClient.get_client("node")

    def accept_pool_connection(self, node_sock):
        pool_socket, client_addr = node_sock.accept()
        pool_id = self.reply_node_handshake(pool_socket)
        return pool_socket, client_addr, pool_id

    def open(self):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_sock:
            node_sock.bind(("", self.port or 0))

            self.port = node_sock.getsockname()[1]
            node_sock.listen()

            NodeBackend.logger.info(f"open node backend server : { self.port }")
            pool_socket, client_addr, pool_id = self.accept_pool_connection(node_sock)

            while True:
                signal_emitted = self.node.signal()

                msg = pool_socket.recv(NodeBackend.NODE_PACKET_SIZE)
                NodeBackend.logger.info(
                    f"node#{self.port} received msg: {msg.decode('utf8')}"
                )
                self.api.call(pool_id, pool_socket, msg)
                sleep(3)

            pool_socket.close()

    def reply_node_handshake(self, sock):
        return self.api.reply_node_handshake(sock)


class NodeApiImpl:
    def __init__(self, node, backend, *args, **kwargs):
        self.node = node
        self.opmap = {}

        self.backend = backend
        self.logger = get_logger(self.node._id)

    def reply_node_handshake(self, sock):
        msg = sock.recv(1024)
        msg = msg.decode("utf8")

        bcursor = BufferCursor(msg)

        op_code = bcursor.advance(OP_PREFIX_LEN)
        ts = bcursor.advance(TIMESTAMP_STR_LEN)
        pool_id = bcursor.advance(POOL_ID_LEN)
        nid = bcursor.advance(NODE_REGISTER_ID_LEN)

        np_connection = {
            "node_register_id": nid,
            "pool_id": pool_id,
            "sock": sock,
            "ts": ts,
        }

        self.backend.poolconnectionmap[pool_id] = np_connection

        NodeBackend.logger.info(f"({pool_id}, {nid}): {msg}")
        reply_msg = "@initn".encode("utf8")

        sock.send(reply_msg)

        return pool_id

    def parse_op_type(self, request_msg):
        op_type = request_msg[:6]
        return op_type

    def reply_sync_node_signal(self, sock, pool_id, request_msg, *args, **kwargs):
        np_connection = self.backend.poolconnectionmap[pool_id]

        msg = "@snsig"
        msg += self.node.signal_id
        msg += get_timestamp()
        msg += str(self.node._id)

        msg += np_connection["node_register_id"]

        signal = self.node.serialize_flags()
        msg += signal
        msg = padding_msg(msg, 32)

        self.logger.info(f"reply: {msg} $len:{len(msg)}")

        msg = msg.encode("utf8")
        sock.send(msg)

    def call(self, pool_id, sock, request_msg):
        request_msg = request_msg.decode("utf8")
        op_type = self.parse_op_type(request_msg)
        request_msg = request_msg[6:]

        if op_type == "!snsig":  # sync_node_signal
            self.reply_sync_node_signal(sock, pool_id, request_msg)

        elif op_type == "!gnsig":
            self.reply_get_node_signal(sock, pool_id, request_msg)


def create_node(port=None):
    node_id = NodeUUIDGenerator.getid()
    node = Node(node_id)
    node.init_node()
    backend = NodeBackend(node, port=port)
    backend.init_node_backend()

    return node_id, node, backend


class Node:
    logger = get_logger("FNode")

    def __init__(self, node_id):
        self._id = node_id
        self.flags = [0, 0, 0, 0, 0, 0]
        self.committer = NodeBlockCommitter()

        self.last_blinked = None

        self.signal_id = None

    def init_node(self):
        self.update_signal()

    def update_signal(self):
        self.signal_id = NodeUUIDGenerator.getsigid()

        for idx, name in enumerate(FLAG_NAMES):
            _p = fp(name)
            self.flags[idx] = int(_p)

    def serialize_flags(self):
        signal = reduce(lambda x, y: x + f"{self.flags[y]}", range(len(self.flags)), "")
        return padding_msg(signal, 10, "0")

    def get_signal(self):
        return self.serialize_flags()

    def after_emit(self):
        self.signal_emitted = False

    def signal(self):
        now = time.time()
        diff = now - (self.last_blinked or now)
        noise = float(randint(0, 100)) / 100.0

        if diff < 5 + noise:
            return None

        signal_emitted = self.serialize_flags()

        self.last_blinked = now

        self.update_signal()
        return signal_emitted

class Simulator:
    def __init__(self):
        pass

    def simulate(self, try_cnt, pv):
        p = Pool(100)
        p.init_pool()
        success_cnt = 0

        for _ in range(try_cnt):
            r = p.get(pv)

            if r:
                success_cnt += 1

            if r % 5 == 0:
                p.init_pool()

        return success_cnt

    def scenario_1(self):
        # 1/9
        p = Pool()
        p.init_pool()
        success_cnt = 0
        try_cnt = 10000

        for _ in range(try_cnt):
            nodes = p.select_nodes(2)
            r = p.gte(nodes, "1/3", 1)

            if r:
                success_cnt += 1

            if r % 5 == 0:
                p.init_pool()

        return success_cnt


class FotuneTimer:
    def __init__(self):
        pass


if __name__ == "__main__":

    import sys

    port = int(sys.argv[1])

    _, _, nodebackend = create_node(port=port)
    nodebackend.open()
