import leveldb
from fortunate_system_const import *
from utils import get_logger, BufferCursor
import pandas as pd

def unix_timestamp_to_ts(t):
    return pd.Timestamp(int(t) * 10)

class BlockParser:
    
    @classmethod
    def parse_signal_tail_buffer(cls, tail):
        cursor = BufferCursor(tail)
        node_id = cursor.advance(NODE_ID_LEN)

        # node_register_id = scursor.advance(NODE_REGISTER_ID_LEN)
        node_flag_field = cursor.advance(NODE_FLAG_FIELD_LEN)
        detail = cursor.rest()
        
        return {
            "node_id": node_id,
            "node_flag_field": node_flag_field,
            #"detail": detail,
        }
    
    @classmethod
    def parse_event_tail_buffer(cls, tail):
        cursor = BufferCursor(tail)
        buffer = cursor.advance(NODE_ID_LEN)
        nodes = []
        for i in range(2):
            nodes.append(buffer)
            buffer = cursor.advance(NODE_ID_LEN)

        return {
            "nodes": nodes
        }
        
    
    @classmethod
    def parse_record(cls, record_buffer):
        scursor = BufferCursor(record_buffer)
        print(scursor.buffer)
        #sign_key = scursor.advance(POOL_SIGN_KEY_LEN)

        record_secondary_id = scursor.advance(NODE_SIGNAL_ID_LEN)
        record_secondary_ts = scursor.advance(TIMESTAMP_STR_LEN)
        record_secondary_ts = unix_timestamp_to_ts(record_secondary_ts)

        record_type = scursor.advance(RECORD_TYPE_FLAG_LEN)

        header = {
            "sign_key": '',
            "record_secondary_id": record_secondary_id,
            "record_secondary_ts": record_secondary_ts,
            "record_type": record_type,
        }

        tail_buffer = scursor.rest()

        if record_type == "01":
            d = cls.parse_signal_tail_buffer(tail_buffer)
        
        elif record_type == "02":
            d = cls.parse_event_tail_buffer(tail_buffer)
        else:
            d = {}
        
        return {**header, **d}

    @classmethod
    def parse(cls, block):
        block = block.decode("utf8")
        bcursor = BufferCursor(block)
        buffer = bcursor.advance(256)
        print(buffer)
        while buffer and len(buffer) == 256:
            buffer = bcursor.advance(256)
            print(buffer)
        return 

        header_cursor = BufferCursor(bcursor.advance(256))

        block_ts = header_cursor.advance(TIMESTAMP_STR_LEN)
        pool_sign_key = header_cursor.advance(POOL_SIGN_KEY_LEN)

        sig_count_field = header_cursor.advance(SIGNAL_COUNT_FIELD)
        records = []

        for _ in range(int(sig_count_field)):
            record_buffer = bcursor.advance(BLOCK_RECORD_LEN)
            record = cls.parse_record(record_buffer)
            records.append(record)

        return block_ts, pool_sign_key, records


if __name__ == "__main__":
    sign_key = "abcdefge"

    db = leveldb.LevelDB("block")
    block = db.Get(sign_key.encode())

    bp = BlockParser()
    block_ts, pool_sign_key, signals = bp.parse(block)
    print(block_ts, pool_sign_key)

    for s in signals:
        print(s)
