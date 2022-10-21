from node import create_node, NodeApiImpl
from pool import create_poolbackend, sync_proc
from multiprocessing import Process
from blockstorageserver import BlockStorageServer
from time import sleep
import fire

PROCS = []

def terminate_procs(procs):
    for p in procs:
        p.terminate()

def get_poolbackend():
    backend = create_poolbackend()
    return backend

def node_process(port):
    try:
        _, n, node_backend = create_node(port)
        node_backend.open()
    except KeyboardInterrupt:
        pass

def poolprocfunction(pool):
    try:
        ip = "127.0.0.1"

        addr0 = (ip, 5050)
        pool.make_node_connection(addr0)

        addr1 = (ip, 5051)
        pool.make_node_connection(addr1)
        
        sync_proc(pool)
    except KeyboardInterrupt:
        pass

def _runblockstorageserver():
    bss = BlockStorageServer()
    bss.app()

class TestSuit:

    def test_node(self):
        node_process(5050)

    def test_node_parse_buffer(self):
        buffer1 = "1666252618631419007304baa27ddfce27"
        expected = "7ddfce27"

        result = NodeApiImpl.parse_signal_id_from_buffer(buffer1)
        print(result, expected)

        assert result == expected


    def test_node_pool_handshake(self):
        nproc1 = Process(target=node_process, args=(5050,))
        nproc1.start()

        nproc2 = Process(target=node_process, args=(5051,))
        nproc2.start()

        poolbackend = get_poolbackend()

        poolproc = Process(target=poolprocfunction, args=(poolbackend, ))
        poolproc.start() 

        PROCS.extend([nproc1, nproc2, poolproc])
    
        return nproc1, nproc2, poolproc

    def test_node_signal_insert(self):
        blockserverproc = Process(target=_runblockstorageserver)
        blockserverproc.start()
        sleep(1.3)

        nproc1 = Process(target=node_process, args=(5050,))
        nproc1.start()

        nproc2 = Process(target=node_process, args=(5051,))
        nproc2.start()

        sleep(1.5)
        
        ip = "127.0.0.1"
        
        addr0 = (ip, 5050)
        addr1 = (ip, 5051)

        poolbackend = get_poolbackend()
        poolbackend.make_node_connection(addr0)
        poolbackend.make_node_connection(addr1)

        for _ in range(10):
            poolbackend.get_node_signal(0)
            poolbackend.sync_node_signal(0)
            poolbackend.sync_node_signal(1)
            sleep(0.5)        

        poolbackend.proc_flush_block()
        nproc1.terminate()

    

if __name__ == "__main__":
    try:
        fire.Fire(TestSuit)
        
    except KeyboardInterrupt:
        for p in PROCS:
            p.terminate()

