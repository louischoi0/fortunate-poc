import hashlib
from node import create_node
import socket
from utils import get_logger, get_timestamp, padding_msg
from time import sleep

POOL_NODE_NUM = 2 ** 8

class PoolBackend:
    logger = get_logger("PoolBackend")

    def __init__(self, pool):
        self.pool = pool
        self.sock = None

    def open(self):
        pass

    def make_node_connection(self, nodeaddr):
        node_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        node_sock.connect(nodeaddr)
        
        return node_sock

    def ready(self):
        ip = '127.0.0.1'
        
        addr0 = (ip, 5050)
        c0 = self.make_node_connection(addr0)
    
        addr1 = (ip, 5051)
        c1 = self.make_node_connection(addr1)

        self.pool.nodemap[5050] = c0
        self.pool.nodemap[5051] = c1

        while True:
            for nodekey in self.pool.nodemap:
                self.sync_node_signal(nodekey)
            sleep(3)

    def init_node_connection(self, nid, *args, **kwargs):
        PoolBackend.logger.info("call:init_node_connection.")
        t = get_timestamp()

        nodesock = self.pool.nodemap[nid]
        opcode = '#00001'
        msg = opcode + t + ""

    def sync_node_signal(self, nid, *args, **kwargs):
        logger = PoolBackend.logger
        PoolBackend.logger.info("")
        t = get_timestamp()

        nodesock = self.pool.nodemap[nid]
        opcode = '#00002'
        msg = opcode + t + ""
        msg = padding_msg(msg, 128)
        msg = msg.encode('utf8')
        
        logger.info(f'call:sync_node_signal $len:{len(msg)}')

        nodesock.send(msg)
        response = nodesock.recv(128)
        response = response.decode("utf8") 

        signal = response[6:44]

        # self.verify_signal(signal)
        self.pool.insert_signal(signal)

        PoolBackend.logger.info(f'received: {signal}')

class NodeSelector:

    def __init__(self, sign_token):
        self.sign_token = sign_token

    def __call__(self, num):
        return self.generate_node_indices(num)

    def generate_node_indices(self, node_num):
        hashed = hashlib.sha256(self.sign_token.encode("utf8")) 
        hashed = hashed.hexdigest()

        return string_to_number_sequence(hashed, node_num)


class Pool:
    MAX_NODE_NUM = 2 ** 8
    INIT_NODE_NUM = 2 ** 6

    def __init__(self):
        self.node_num = POOL_NODE_NUM
        self.nodes = []

        self.node_con_info = {}
        self.nodemap = {}

        self.sign_key = None
        self.signal_chain = {}

        self.init_pool()

    def insert_signal(self, signal):
        sign_key = self.sign_key

        if sign_key not in self.signal_chain :
            self.signal_chain[sign_key] = [signal]
        else:
            self.signal_chain[sign_key].append(signal)

    def init_pool(self):
        self.nodes = []
        self.sign_key = "abcdefge"
        #self.selector = NodeSelector(self.sign_key)

    def select_nodes(self, num):
        a = [*range(len(self.nodes))]
        result = []
        for _ in range(num):
            node_selected = choice(a)
            result.append(self.nodes[node_selected])
        
        return result

    def mul(self, nodes, flag):
        flags = [*map(lambda x : x.flags[FLAG_POSITION[flag]], nodes)]
        return reduce(lambda y, x: y * x, flags, 1)

    def cnt(self, nodes, flag):
        flags = [*map(lambda x : int(x.flags[FLAG_POSITION[flag]]) , nodes)]
        return sum(flags)

    def gte(self, nodes, flag, c):
        out_c = self.cnt(nodes, flag) 
        return out_c >= c

    def get(self, n):
        if n == "1/1000" :
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
    return PoolBackend(p)    


if __name__ == "__main__":
    create_poolbackend()


