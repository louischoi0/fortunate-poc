from multiprocessing import Process
from time import sleep
import fire

from event import FortunateServer
from node import create_node
from pool import create_poolbackend, sync_proc
from blockstorageserver import BlockStorageServer

PROCS = []

def get_poolbackend():
    backend = create_poolbackend('aaaabbbb')
    return backend

def _runnode(port):
    import node
    _, _, nodebackend = node.create_node(port)
    nodebackend.open()

def node_process(port):
    try:
        _, n, node_backend = create_node(port)
        node_backend.open()
    except KeyboardInterrupt:
        pass

def _runblockstorageserver():
    bss = BlockStorageServer()
    bss.app()
    return bss

class BlockStorageTestSuit:

    def test_insert_singal_record(self):
        blockserverproc = Process(target=_runblockstorageserver)
        blockserverproc.start()

        sleep(1.3)

        nproc1 = Process(target=node_process, args=(5050,))
        nproc1.start()

        nproc2 = Process(target=node_process, args=(5051,))
        nproc2.start()
        PROCS.extend([nproc1, nproc2])
        sleep(1.5)

        ip = "127.0.0.1"
        
        addr0 = (ip, 5050)
        addr1 = (ip, 5051)

        poolbackend = get_poolbackend()
        poolbackend.make_node_connection(addr0)
        poolbackend.make_node_connection(addr1)

        for _ in range(3):
            poolbackend.sync_node_signal(0)
            poolbackend.sync_node_signal(1)
            sleep(0.3)

        poolbackend.write_block(poolbackend.pool.sign_key)
        poolbackend.blockstorage_client.api.request_commit_block(poolbackend.pool.sign_key)

if __name__ == "__main__":
    try:
        fire.Fire(BlockStorageTestSuit)
        
    except KeyboardInterrupt:
        for p in PROCS:
            p.terminate()

