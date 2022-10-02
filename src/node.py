from random import randint, choice
from functools import reduce
import hashlib
import socket
import leveldb
import time
import logging
from utils import get_logger, padding_msg

FLAG_NAMES = [ "1/2","1/4", "1/8", "1/10", "1/100", "1/3"  ]
FLAG_POSITION = {
    "1/2": 0,
    "1/4": 1,
    "1/8": 2,
    "1/10": 3,
    "1/100": 4,
    "1/3": 5
}

FLAG_THRES = {
    "1/2": 5000,
    "1/4": 2500,
    "1/5": 2000,
    "1/8": 1250,
    "1/10": 1000,
    "1/100": 100,
    "1/3": 3333, 
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
    def getid(self):
        seed = time.time()    
        hasher = hashlib.sha256()
        hasher.update(str(seed).encode('utf8'))
        return hasher.hexdigest()

# 서버 소켓 설정
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

        self.api = NodeApiImpl(node)

    def open(self): 

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_sock:
            node_sock.bind(('', self.port or 0))

            self.port = node_sock.getsockname()[1]
            node_sock.listen() 

            NodeBackend.logger.info(f"open node backend server : { self.port }")
            pool_socket, cient_addr = node_sock.accept() 

            while True:
                signal_emitted = self.node.blink()
                print("accept")

                msg = pool_socket.recv(NodeBackend.NODE_PACKET_SIZE) 
                NodeBackend.logger.info(f"node#{self.port} received msg: {msg.decode('utf8')}")
                self.api.call(pool_socket, msg)
                print("hi") 
                from time import sleep
                sleep(3)

            pool_socket.close() 

class NodeApiImpl:

    def __init__(self, node):
        self.node = node
        self.opmap = {}

        self.logger = get_logger(self.node._id)

    def parse_op_type(self, request_msg):
        op_type = request_msg[:6]
        return op_type

    def reply_sync_node_signal(self, sock):
        msg = "$00002"
        signal = self.node.serialize_flags()
        msg += signal
        msg = padding_msg(msg, 32)

        msg = msg.encode('utf8')
        self.logger.info(f"reply: {msg} $len:{len(msg)}")

        sock.send(msg)
        
    def call(self, sock, request_msg):
        request_msg = request_msg.decode('utf8')
        op_type = self.parse_op_type(request_msg)

        if op_type == "#00002": # sync_node_signal
            self.reply_sync_node_signal(sock) 

        elif op_type == "#00001":
            sock.send("init_node_connection".encode('utf8'))


def create_node(port=None):
    node_id = NodeUUIDGenerator.getid()[:8]
    node = Node(node_id) 
    backend = NodeBackend(node, port=port)
    return node_id, node, backend 

class Node :
    logger = get_logger("FNode")

    def __init__(self, node_id):
        self._id = node_id
        self.flags = [ 0, 0, 0, 0, 0, 0 ]     
        self.committer = NodeBlockCommitter()

        self.last_blinked = None

    def update_flags(self) :

        for idx, name in enumerate(FLAG_NAMES):
            _p = fp(name)
            self.flags[idx] = int(_p)

        ns = self.serialize_flags()

    def serialize_flags(self):
        return reduce(lambda x,y: x + ";" + f"{FLAG_NAMES[y]}:{self.flags[y]}", range(len(self.flags)), "")[1:]

    def to_record(self):
        return self.serialize_flags()

    def after_emit(self):
        self.signal_emitted = False

    def blink(self):
        now = time.time()
        diff = now - (self.last_blinked or now)
        noise = float(randint(0, 100)) / 100.

        if diff < 5 + noise: return None

        signal_emitted = self.serialize_flags()

        self.last_blinked = now

        self.update_flags() 
        return signal_emitted

def string_to_number_sequence(input_str, num):
    input_str = input_str[:num]
    return [ *map(lambda x: ord(x), input_str) ]


class SignTokenGenerator:
    def __init__(self, sign_key):
        self.sign_key = sign_key

    def next(self, seed):
        before = self.sign_key
        self.sign_key = str(hash(before))

class Fortune:
    def __init__(self):
        self.pool = Pool()
        self.pool.init_pool()

        self.initial_sign_key = "abcdefghjklmnopqrstuvwxyz"

        self.nselector = NodeSelector(self.initial_sign_key)
    
    def api(self, req):
        # 45%
        nodes_selected = self.nselector(2) 
        nodes = (( self.pool.nodes[x] for x in nodes_selected ))
        return self.pool.execute_op("0.45")

class SignalBlock:
    def __init__(self):
        pass

class Block:
    def __init__(self):
        pass

class Verifier:
    def __init__(self):
        pass

class Simulator:
    def __init__(self):
        pass

    def simulate(self, try_cnt, pv):
        p = Pool(100)
        p.init_pool()
        success_cnt = 0

        for _ in range(try_cnt):
            r = p.get(pv)

            if r :
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

            if r :
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

    _, _, nodebackend  = create_node(port=port)
    nodebackend.open()
