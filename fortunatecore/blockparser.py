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
        
        signal_id = cursor.advance(NODE_SIGNAL_ID_LEN)
        node_id = cursor.advance(NODE_ID_LEN)
        node_flag_field = cursor.advance(NODE_FLAG_FIELD_LEN)
        detail = cursor.rest()
        
        return {
            "signal_id": signal_id,
            "node_id": node_id,
            "flag_field": node_flag_field,
        }
    
    @classmethod
    def parse_tf_event_tail_buffer(cls, record_buffer):
        cursor = BufferCursor(record_buffer)

        sign_key = cursor.advance(POOL_SIGN_KEY_LEN)
        event_hash = cursor.advance(EVENT_HASH_LEN)
        
        serialized_at = cursor.advance(TIMESTAMP_STR_LEN)
        state = cursor.advance(1)

        signals = [] 
        node_sig = cursor.advance(NODE_SIGNAL_ID_LEN)

        while node_sig != "00000000":
            signals.append(node_sig)
            node_sig = cursor.advance(NODE_SIGNAL_ID_LEN)
        
        return {
            "sign_key": sign_key,
            "event_hash": event_hash,
            "state": state,
            "serialized_at": serialized_at,
            "signals": signals,
        }
    
    @classmethod
    def parse_record(cls, record_buffer):
        scursor = BufferCursor(record_buffer)
        #sign_key = scursor.advance(POOL_SIGN_KEY_LEN)

        record_type = scursor.advance(RECORD_TYPE_FLAG_LEN)
        ts = scursor.advance(TIMESTAMP_STR_LEN)

        header = {
            "record_type": record_type,
            "ts": pd.Timestamp(int(ts)*10),
        }

        tail_buffer = scursor.rest()

        if record_type == "02":
            d = cls.parse_signal_tail_buffer(tail_buffer)
        
        elif record_type == "01":
            d = cls.parse_tf_event_tail_buffer(tail_buffer)

        elif record_type == "00":
            d = {}

        else:
            d = {}
        
        return {**header, **d}

    @classmethod
    def parse(cls, block):
        block = block.decode("utf8")
        bcursor = BufferCursor(block)
        buffer = bcursor.advance(256)
        records = []

        while buffer and len(buffer) == 256:
            print(buffer)
            record = cls.parse_record(buffer)
            records.append(record) 
            buffer = bcursor.advance(256)
        
        return records

if __name__ == "__main__":
    sign_key = "abcdefge"

    db = leveldb.LevelDB("block")
    block = db.Get(sign_key.encode())

    bp = BlockParser()
    records = bp.parse(block)

    for r in records:
        print(r)

    """
    block_ts, pool_sign_key, signals = bp.parse(block)
    print(block_ts, pool_sign_key)

    for s in signals:
        print(s)
    """
