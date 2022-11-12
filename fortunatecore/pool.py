import hashlib
import threading
from node import create_node, NodeApiImpl
import socket
from utils import get_logger, get_timestamp
from utils import padding_msg_front, padding_msg, strpshift
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
            backend.logger.info(f"nodekey: {nodekey}")
            backend.sync_node_signal(nodekey)

        sleep(2.5)

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

    def make_node_connections(self, ports):
        ip = "127.0.0.1"

        for port in ports:
            addr0 = (ip, port)
            self.make_node_connection(addr0)
        
    def ready(self, ports=(5050, 5051)):
        self.make_node_connections(ports)

        return self.fork_proc_sync_nodes()

    def fork_proc_sync_nodes(self, *args, **kwargs):
        th = threading.Thread(target=sync_proc, args=(self,))
        self.sync_node_thread = th
        PoolBackend.logger.info("Fork thread for sync node signals")
        th.start()
        return th

    def terminate(self):
        #self.sync_node_thread.exit()
        pass


    def node_handshake(self, nodesock, *args, **kwargs):
        PoolBackend.logger.info("call:node_handshake.")
        init_msg = nodesock.recv(8192)  
        init_msg = init_msg.decode('utf8')

        assert init_msg[:6] == "@echid"
        node_id_echo = init_msg[6:]

        node_cnt = len(self.pool.nodemap.keys())
        nid = node_cnt
        t = get_timestamp()

        node_detail = {
            "sock": nodesock,
            "nodeseq": nid,
            "_id": node_id_echo,
            "connected_at": t,
        }

        self.pool.nodemap[nid] = nodesock
        self.pool.nodedetailmap[nid] = node_detail

        opcode = "!initn"
        nidmsg = node_id_echo #TODO node_id_echo -> nid

        msg = opcode + t + self.pool._id + nidmsg
        msg = msg.encode("utf8")
        PoolBackend.logger.info(
            f"init_node_connection: handshake is completed. msg: {msg}"
        )

        nodesock.send(msg)
        response_msg = nodesock.recv(8192)
        response_msg = response_msg.decode('utf8') 

        response_op_type = response_msg[:6]

        assert response_op_type == "@initn"

        self.node_count += 1

    def get_node_signal(self, register_id, *args, **kwargs):
        logger = self.logger
        t = get_timestamp()

        nid = register_id

        nodesock = self.pool.nodemap[nid]
        opcode = "!snsig"

        msg = opcode + t 
        msg = padding_msg(msg, 256)
        msg = msg.encode("utf8")

        logger.debug(f"call:get_node_signal $nodeid: {nid},  $len:{len(msg)}")

        nodesock.send(msg)
        response = nodesock.recv(8192)
        response = response.decode("utf8")

        recv_op_code = response[:6]

        assert recv_op_code == "@snsig"
        signal = response[6:]

        signal_obj = NodeApiImpl.parse_signal(signal)
        self.logger.info(f"signal dict: {signal_obj} from {signal}")
        return signal

    def sync_node_signal_all(self, *args, **kwargs):
        for i, n in enumerate(nodes):
            self.sync_node_signal(i)

    def sync_node_signal(self, nid, *args, **kwargs):
        self.pool.acquire_sign_key_lock()
        
        signal = self.get_node_signal(nid, *args, **kwargs)
        signal_id = NodeApiImpl.parse_signal_id_from_buffer(signal)
        
        if signal_id in self.pool.signal_bloomfilter: return
        
        self.pool.signal_bloomfilter[signal_id] = signal

        signal = strpshift(signal, "02")
        sign_key = self.pool.sign_key
        
        self.pool.release_sign_key_lock()
        self.pool.insert_signal(signal)
        #self.blockstorage_client.api.request_insert_block_row(sign_key, signal)


    def write_block(self, sign_key):
        buffer = self.pool.impl.serialize_chain()
        assert len(buffer) % BLOCK_RECORD_LEN == 0
        self.logger.debug(f"Write block buffer")
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
    
    def update_sign_key(self, sign_key, *args, **kwargs):
        pass
        
    
    def serialize_chain(self):
        self.pool.acquire_sign_key_lock()
        sign_key = self.pool.sign_key
        materialize_at = get_timestamp()

        chain = self.pool.signal_chain[sign_key]

        buffer = ""
        buffer += str(materialize_at)

        buffer += self.pool.sign_key
        buffer = padding_msg(buffer, BLOCK_RECORD_LEN)
        buffer = strpshift(buffer, '00').encode('utf8')

        assert len(buffer) == BLOCK_RECORD_LEN

        for signal in chain:
            signal = padding_msg(signal, BLOCK_RECORD_LEN)
            buffer += signal.encode("utf8")

        assert len(buffer) % BLOCK_RECORD_LEN == 0
        self.pool.release_sign_key_lock()
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

        self.signal_bloomfilter = {}

        self.sign_key_lock = threading.Lock()
        self.previous_sign_key = None

    def acquire_sign_key_lock(self):
        self.logger.info(f"acquire sign key lock")
        return
        return self.sign_key_lock.acquire()

    def release_sign_key_lock(self):
        self.logger.info(f"release sign key lock")
        return
        return self.sign_key_lock.release()

    def update_sign_key(self, new_sign_key, *args, **kwargs):
        self.acquire_sign_key_lock()
        self.previous_sign_key = self.sign_key

        self.sign_key = new_sign_key
        self.pool.signal_bloomfilter = {}

        self.impl.update_sign_key(new_sign_key)
        
        self.release_sign_key_lock()
    
    def allocate_new_block(new_sign_key):
        """
        This method must be called after commit previous block.
        """
        self.logger.info(f"allocate new block - sign_key: {new_sign_key}")
        self.update_sign_key(new_sign_key)
        self.sync_node_signal_all()

    def insert_event(self, event):
        sign_key = self.sign_key

        self.logger.debug("call event_signal")

        if sign_key not in self.signal_chain:
            self.logger.debug(f"initialize signal_chain sign_key: {sign_key} ")
            self.signal_chain[sign_key] = [signal]
        else:
            self.logger.debug(f"insert event to {sign_key} : {event}")
            self.signal_chain[sign_key].append(event)

    def insert_signal(self, signal):
        sign_key = self.sign_key
        
        self.logger.debug("call insert_signal")

        if sign_key not in self.signal_chain:
            self.logger.debug(f"initialize signal_chain sign_key: {sign_key} ")
            self.signal_chain[sign_key] = [signal]
        else:
            self.logger.debug(f"insert signal to {sign_key} : {signal}")
            self.signal_chain[sign_key].append(signal)

    def init_pool(self):
        self.impl = PoolImpl(self)

        self.nodes = []
        self.sign_key = self.sign_key

        self._id = PoolUUIDGenerator.getid()
        self.selector = NodeSelector(self.sign_key)

        self.logger = get_logger(f"Pool#{self._id}")

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


def create_poolbackend(sign_key):
    p = Pool(sign_key=sign_key)
    p.init_pool()
    backend = PoolBackend(p)
    backend.init_backend()
    return backend


if __name__ == "__main__":
    create_poolbackend('aaaabbbb')
