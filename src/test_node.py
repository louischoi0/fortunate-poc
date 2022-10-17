from node import create_node
from pool import create_poolbackend, sync_proc
from multiprocessing import Process
from time import sleep
import fire

PROCS = []

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

class TestSuit:
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
        nproc1 = Process(target=node_process, args=(5050,))
        nproc1.start()

        poolbackend = get_poolbackend()
        
        ip = "127.0.0.1"

        sleep(1.5)
        addr0 = (ip, 5050)
        poolbackend.make_node_connection(addr0)

        for _ in range(20):
            poolbackend.sync_node_signal(0)
            sleep(0.8)        

        poolbackend.proc_flush_block()
        return [ nproc1 ]
    

if __name__ == "__main__":
    try:
        fire.Fire(TestSuit)
        
    except KeyboardInterrupt:
        for p in PROCS:
            p.terminate()

    

    """
    try:
        p1,p2,p3 = test_node_pool_handshake()
        
    except KeyboardInterrupt:
        p1.terminate()
        p2.terminate()
        p3.terminate()
    """        


    






