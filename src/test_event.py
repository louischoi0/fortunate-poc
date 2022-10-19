from multiprocessing import Process
from time import sleep
import fire

from event import FortunateServer
from node import create_node
from pool import create_poolbackend, sync_proc

PROCS = []

def _runnode(port):
    import node
    _, _, nodebackend = node.create_node(port)
    nodebackend.open()

class EventTestSuit:

    def test_insert_event(self):
        p0 = Process(target=_runnode, args=(5050,))
        p0.start()

        p1 = Process(target=_runnode, args=(5051,))
        p1.start()
        
        sleep(3)

        server = FortunateServer()
        poolproc = server.init_server()
        server.connect_block_server()

        poolbackend = server.pool.backend

        for _ in range(3):
            poolbackend.sync_node_signal(0)
            poolbackend.sync_node_signal(1)
            sleep(0.5)        

        
        event_request = {
            "event_type": "tf",
            "event_secondary_type": "1;4",
            "event_hash": "thisisev",
        }

        server.api(event_request)
        server.blockstorage_client.api.request_commit_block(server.pool.sign_key)

        p0.terminate()
        p1.terminate()

if __name__ == "__main__":
    try:
        fire.Fire(EventTestSuit)
    except KeyboardInterrupt:
        for p in PROCS:
            p.terminate()



