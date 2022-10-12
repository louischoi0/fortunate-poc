import leveldb
from fortunate_system_const import *
from utils import get_logger, BufferCursor


class BlockParser:
    @classmethod
    def parse(cls, block):
        block = block.decode("utf8")
        print(block)

        bcursor = BufferCursor(block)

        block_ts = bcursor.advance(TIMESTAMP_STR_LEN)
        pool_sign_key = bcursor.advance(POOL_SIGN_KEY_LEN)

        sig_count_field = bcursor.advance(SIGNAL_COUNT_FIELD)
        records = []

        for _ in range(int(sig_count_field)):
            record_buffer = bcursor.advance(BLOCK_RECORD_LEN)

            scursor = BufferCursor(record_buffer)

            ts_0 = scursor.advance(TIMESTAMP_STR_LEN)
            sign_key = scursor.advance(POOL_SIGN_KEY_LEN)

            signal_id = scursor.advance(NODE_SIGNAL_ID_LEN)
            signal_ts = scursor.advance(TIMESTAMP_STR_LEN)
            record_type = scursor.advance(RECORD_TYPE_FLAG_LEN)
            node_id = scursor.advance(NODE_ID_LEN)

            # node_register_id = scursor.advance(NODE_REGISTER_ID_LEN)
            node_flag_field = scursor.advance(NODE_FLAG_FIELD_LEN)
            detail = scursor.rest()

            sig_detail = {
                "ts_0": ts_0,
                "record_type": record_type,
                "sign_key": sign_key,
                "signal_ts": signal_ts,
                "node_id": node_id,
                "node_flag_field": node_flag_field,
                "detail": detail,
            }

            records.append(sig_detail)

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
