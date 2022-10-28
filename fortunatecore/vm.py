import node
from pool import Pool
import pool

from multiprocessing import Process


def _runnode(port):
    _, _, nodebackend = node.create_node(port)
    nodebackend.open()


def _runpool():
    poolbackend = pool.create_poolbackend()
    poolbackend.ready()


if __name__ == "__main__":
    p0 = Process(target=_runnode, args=(5050,))
    p0.start()

    p1 = Process(target=_runnode, args=(5051,))
    p1.start()

    from time import sleep

    sleep(2)
    _runpool()

    p0.join()
