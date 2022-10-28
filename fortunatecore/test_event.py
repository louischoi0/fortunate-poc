from multiprocessing import Process
from time import sleep
import fire

from event import FortunateServer
from node import create_node
from pool import create_poolbackend, sync_proc
from blockstorageserver import BlockStorageServer

from random import randint
import threading

PROCS = []
global NODES
NODES = []

def _runnode(port):
    import node
    _, node, nodebackend = node.create_node(port)
    nodebackend.open()

    global NODES
    NODES.append(node)

def _runblockstorageserver():
    bss = BlockStorageServer()
    bss.app()
    return bss

def request_event_thread(server):
    cnt = 0
    while cnt < 5:

        event_request = {
            "event_type": "tf",
            "event_secondary_type": "1;4",
            "event_hash": "event" + str(randint(101,999)),
        }
    
        server.api(event_request)
        cnt += 1
        sleep(1.4)

class EventTestSuit:

    def test_insert_event(self):
        blockserverproc = Process(target=_runblockstorageserver)
        PROCS.append(blockserverproc)
        blockserverproc.start()
        sleep(1.4)

        p0 = Process(target=_runnode, args=(5050,))
        p0.start()

        p1 = Process(target=_runnode, args=(5051,))
        p1.start()

        p2 = Process(target=_runnode, args=(5062,))
        p2.start()

        p3 = Process(target=_runnode, args=(5063,))
        p3.start()
        
        sleep(3)

        server = FortunateServer()

        poolproc = server.init_server((5050,5051, 5062, 5063))
        server.connect_block_server()

        event_thread = threading.Thread(target=request_event_thread, args=(server,))
        event_thread.start()

        poolbackend = server.pool.backend

        for _ in range(10):
            for i in range(3):
                poolbackend.sync_node_signal(i)
            sleep(0.5)        


        poolbackend.write_block(poolbackend.pool.sign_key)
        server.blockstorage_client.api.request_commit_block(server.pool.sign_key)
        
        #event_proc.terminate()
        poolbackend.terminate()
        p0.terminate()
        p1.terminate()
        p2.terminate()
        p3.terminate()
        blockserverproc.terminate()

        global NODES
        for n in NODES:
            n.terminate()

if __name__ == "__main__":
    try:
        fire.Fire(EventTestSuit)
    except KeyboardInterrupt:
        for p in PROCS:
            p.terminate()
        for n in NODES:
            n.terminate()

