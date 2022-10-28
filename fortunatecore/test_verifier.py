import fire
from blockstorageserver import BlockStorageServer
from blockverifier import BlockVerifier
from multiprocessing import Process
from time import sleep

def _runblockstorageserver():
    bss = BlockStorageServer()
    bss.app()
    return bss


class VerifierTestSuit:
    def test_verifier(self):
        blockserverproc = Process(target=_runblockstorageserver)
        blockserverproc.start()

        sleep(1.4)
        
        v = BlockVerifier() 
        v.verify_block('abcdefge')



if __name__ == "__main__":
    fire.Fire(VerifierTestSuit)

