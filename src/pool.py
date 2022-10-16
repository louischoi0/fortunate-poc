import hashlib
import threading
from node import create_node
import socket
from utils import get_logger, get_timestamp
from utils import padding_msg_front, padding_msg
from time import sleep
from fortunate_system_const import *
import leveldb
import time
import random
from blockstorageserver import BlockStorageClient


def generate_number_sequence(sign_key, input_str, num, rand_max):
    random.seed(sign_key + input_str)
    return [*(random.randint(0, rand_max - 1) for _ in range(num))]


def sync_proc(backend):
    while True:

        for nodekey in backend.pool.nodemap:
            backend.sync_node_signal(nodekey)

            # TODO should be flushed by another proc
            backend.proc_flush_block()

        sleep(2.5)
        #signals = pool.get_signals_from_node_pool("event0", 2)

class PoolUUIDGenerator:
    def __init__(self):
        pass

    @classmethod
    def getid(self):
        seed = time.time()
        hasher = hashlib.sha256()
        hasher.update(str(seed).encode("utf8"))
        return hasher.hexdigest()[:8]


class PoolBackend:
    logger = get_logger("PoolBackend")

    def __init__(self, pool):
        self.pool = pool
        self.node_count = 0

    def init_backend(self):
        self.blockstorage_client = BlockStorageClient.get_client("pool0")
        self.pool.backend = self

    def make_node_connection(self, nodeaddr):
        node_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_sock.connect(nodeaddr)

        self.node_handshake(node_sock)

        return node_sock

    def ready(self):
        ip = "127.0.0.1"

        addr0 = (ip, 5050)
        c0 = self.make_node_connection(addr0)

        addr1 = (ip, 5051)
        c1 = self.make_node_connection(addr1)

        self.pool.nodemap[5050] = c0
        self.pool.nodemap[5051] = c1

        return self.fork_proc_sync_nodes()

    def fork_proc_sync_nodes(self, *args, **kwargs):
        th = threading.Thread(target=sync_proc, args=(self,))
        PoolBackend.logger.info("Fork thread for sync node signals")
        th.start()
        return th

    def node_handshake(self, nodesock, *args, **kwargs):
        PoolBackend.logger.info("call:node_handshake.")
        node_cnt = len(self.pool.nodemap.keys())
        nid = node_cnt
        t = get_timestamp()

        node_detail = {
            "sock": nodesock,
            "nodeseq": nid,
            "_id": None,
            "connected_at": t,
        }

        self.pool.nodemap[nid] = nodesock
        self.pool.nodedetailmap[nid] = node_detail

        opcode = "!initn"
        nidmsg = padding_msg_front(nid, NODE_REGISTER_ID_LEN, "0")

        msg = opcode + t + self.pool._id + nidmsg
        msg = msg.encode("utf8")
        PoolBackend.logger.info(
            f"init_node_connection: handshake is completed. msg: {msg}"
        )

        nodesock.send(msg)
        msg = nodesock.recv(8192)
        msg = msg.decode("utf8")
        response_op_type = msg[:6]

        assert response_op_type == "@initn"

        self.node_count += 1

    def get_node_signal(self, register_id, *args, **kwargs):
        logger = PoolBackend.logger
        t = get_timestamp()

        nid = register_id

        nodesock = self.pool.nodemap[nid]
        opcode = "!snsig"

        msg = opcode + t 
        msg = padding_msg(msg, 128)
        msg = msg.encode("utf8")

        logger.debug(f"call:sync_node_signal $len:{len(msg)}")

        nodesock.send(msg)
        response = nodesock.recv(8192)
        response = response.decode("utf8")

        recv_op_code = response[:6]

        assert recv_op_code == "@snsig"
        signal = response[6:]
        return signal

    def sync_node_signal(self, nid, *args, **kwargs):
        signal = self.get_node_signal(nid, *args, **kwargs)
        self.pool.insert_signal(signal)

    def proc_flush_block(self):
        sign_key = self.pool.sign_key
        signals = self.pool.signal_chain[sign_key]

        if len(signals) % 2 == 0:
            self.write_block(sign_key)

        if len(signals) % 5 == 0:
            self.blockstorage_client.api.request_commit_block(sign_key)

    def write_block(self, sign_key):
        buffer = self.pool.impl.serialize_chain()
        self.logger.debug(f"Write block buffer: {buffer}")
        key = sign_key.encode("utf8")

        self.blockstorage_client.api.request_insert_block_row(sign_key, buffer)

    def get_signals_from_node_pool(self, event_hash, signal_num):
        node_indicies = self.pool.impl.get_node_indicies(
            event_hash, signal_num, self.node_count
        )
        signals = []

        for n in node_indicies:
            signal = self.get_node_signal(n)
            signals.append(signal)

        return signals


class NodeSelector:
    def __init__(self, sign_token):
        self.sign_token = sign_token

    def generate_node_indicies(self, event_hash, node_num, node_max_num):
        hashed = hashlib.sha256(self.sign_token.encode("utf8"))
        hashed = hashed.hexdigest()

        return generate_number_sequence(hashed, event_hash, node_num, node_max_num)


class PoolImpl:
    def __init__(self, pool):
        self.pool = pool

    def proc_block_fush(self):
        while True:
            sleep(1)

    def serialize_chain(self):
        sign_key = self.pool.sign_key
        materialize_at = get_timestamp()

        chain = self.pool.signal_chain[sign_key]

        buffer = b""
        buffer += str(materialize_at).encode("utf8")

        buffer += self.pool.sign_key.encode("utf8")

        for signal in chain:
            signal = padding_msg(signal, BLOCK_RECORD_LEN)
            buffer += signal.encode("utf8")
        
        return buffer

    def get_node_indicies(self, event_hash, num, node_cnt):
        return self.pool.selector.generate_node_indicies(event_hash, num, node_cnt)


class Pool:
    MAX_NODE_NUM = 2**8
    INIT_NODE_NUM = 2**6

    def __init__(self, sign_key=None, **kwargs):
        self._id = None

        self.nodes = []

        self.node_con_info = {}
        self.nodemap = {}
        self.nodedetailmap = {}

        self.sign_key = sign_key
        self.signal_chain = {}

        self.sign_key_published_at = None

        self.impl = None
        self.selector = None

        self.backend = None

    def insert_signal(self, signal):
        sign_key = self.sign_key
        self.sign_key_published_at = get_timestamp()

        if sign_key not in self.signal_chain:
            self.signal_chain[sign_key] = [signal]
        else:
            self.signal_chain[sign_key].append(signal)

    def init_pool(self):
        self.impl = PoolImpl(self)

        self.nodes = []
        self.sign_key = self.sign_key or "abcdefge"

        self._id = PoolUUIDGenerator.getid()
        self.selector = NodeSelector(self.sign_key)

    def mul(self, nodes, flag):
        flags = [*map(lambda x: x.flags[FLAG_POSITION[flag]], nodes)]
        return reduce(lambda y, x: y * x, flags, 1)

    def cnt(self, nodes, flag):
        flags = [*map(lambda x: int(x.flags[FLAG_POSITION[flag]]), nodes)]
        return sum(flags)

    def gte(self, nodes, flag, c):
        out_c = self.cnt(nodes, flag)
        return out_c >= c

    def get(self, n):
        if n == "1/1000":
            nodes = self.select_nodes(3)
            return self.is_okay(nodes, "1/10")

        elif n == "1/8":
            nodes = self.select_nodes(3)
            return self.is_okay(nodes, "1/2")

    def execute_op(self, op):
        if op == "0.45":
            nodes = self.select_nodes(2)
            return self.gte(nodes, "1/3", 1)


def create_poolbackend():
    p = Pool()
    p.init_pool()
    backend = PoolBackend(p)
    backend.init_backend()
    return backend


if __name__ == "__main__":
    create_poolbackend()
