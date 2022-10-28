from fastapi import FastAPI
import node
from pool import Pool
import pool
from blockstorageserver import BlockStorageServer
from multiprocessing import Process
from time import sleep
from event import FortunateServer



class InteractiveShell:
    def __init__(self, server):
        self.server = server

    def call_api(self, cmd):
        server = self.server
        t, s, h = cmd
        req = {
            "event_type": t,
            "event_secondary_type": s,
            "event_hash": h
        }

        return server.api(req)

    def interactive(self, *args, **kwargs):

        while True: 
            print("client >> ", end="")
            cmd = input()
            cmd = cmd.split(" ")

            try:
                response = self.call_api(cmd)
            except ValueError:
                continue

            print(response)

app = {}

def _runnode(port):
    _, _, nodebackend = node.create_node(port)
    nodebackend.open()


def _runserver():
    server = FortunateServer()
    server.init_server()
    server.connect_block_server()

    app["server"] = server
    return server


def _runblockstorageserver():
    bss = BlockStorageServer()
    bss.app()
    app["bss"] = bss

if __name__ == "__main__":
    blockserverproc = Process(target=_runblockstorageserver)
    #blockserverproc.start()
    #sleep(1.5)

    p0 = Process(target=_runnode, args=(5050,))
    p0.start()

    p1 = Process(target=_runnode, args=(5051,))
    p1.start()

    sleep(1.5)
    server = _runserver()

    shell = InteractiveShell(server)
    shell.interactive()

