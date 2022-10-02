import hashlib

class Block:
    def __init__(self, pool_id, s_index, t_index, prev_block, sign_key):
        self.s_index = s_index
        self.prev_block = prev_block

        self.created_at = None
        self.updated_at = None

        self.records = []

        self.sign_key = sign_key
        self.blockhash = None
        
    def is_genesis(self):
        return self.prev_block is None

    def insert(self, record):
        self.records.append(record)

    def hash(self):
        s = (self.prev_block and self.prev_block.blockhash) or "genesis"

        hasher = hashlib.sha256(s.encode("utf8"))

        for record in self.records :
            rec = record#.serialize()
            hasher.update(rec.encode("utf8"))
    
        self.blockhash = hasher.hexdigest()
        return self.blockhash

if __name__ == "__main__":
    genblock = Block(1,2,3,None, "abcdefg")

    block1 = Block(1,2,3,genblock, "hijklmn")

    genblock.insert("a")
    genblock.insert("b")
    genblock.insert("c")

    block1.insert("d")
    block1.insert("f")

    a = genblock.hash() 
    
    print(a)

    b = block1.hash()
    
    print(b)

