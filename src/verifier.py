import leveldb
from blockparser import BlockParser
from pool import generate_number_sequence


class Verifier:
    def __init__(self):
        self.db = leveldb.LevelDB("block")

    def verify(self, block_sign_key):
        block = self.db.Get(block_sign_key.encode())
        block_ts, pool_sign_key, signals = BlockParser.parse(block)
        print(block_ts, pool_sign_key)

        for s in signals:
            print(s)


if __name__ == "__main__":
    sign_key = "abcdefge"
    input_hash = "xyz"

    vf = Verifier()
    vf.verify(sign_key)

    indicies = generate_number_sequence(sign_key, input_hash, 3, 1)
    print(indicies)
