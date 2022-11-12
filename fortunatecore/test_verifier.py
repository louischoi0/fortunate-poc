import fire
from blockstorageserver import BlockStorageServer
from blockverifier import BlockVerifier
from multiprocessing import Process
from time import sleep


class VerifierTestSuit:
    def test_verifier(self):
        v = BlockVerifier() 
        v.verify_block('abcdefge')



if __name__ == "__main__":
    fire.Fire(VerifierTestSuit)

