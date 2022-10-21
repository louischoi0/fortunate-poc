import fire
from blockstorageserver import BlockStorageServer
from blockverifier import Verifier

def _runblockstorageserver():
    bss = BlockStorageServer()
    bss.app()
    return bss


class VerifierTestSuit:
    def test_verifier(self):
        blockserverproc = Process(target=_runblockstorageserver)
        PROCS.append(blockserverproc)
        blockserverproc.start()

        sleep(1.4)
        
        v = Verifier() 
        v.verify_event('abcdefge') 



if __name__ == "__main__":
    fire.Fire(VerifierTestSuit)

