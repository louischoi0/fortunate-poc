import leveldb
from fortunate_system_const import *
from utils import get_logger, BufferCursor


class BlockParser:
    @classmethod
    def parse(cls, block):
        block = block.decode("utf8")
        print(block)
        bcursor = BufferCursor(block)
        sign_key_len = POOL_SIGN_KEY_LEN
        ts_len = TIMESTAMP_STR_LEN

        block_ts = bcursor.advance(ts_len)
        pool_sign_key = bcursor.advance(sign_key_len)

        sig_count_field = bcursor.advance(SIGNAL_COUNT_FIELD)

        signals = []

        for _ in range(int(sig_count_field)):
            signal_id = bcursor.advance(NODE_SIGNAL_ID_LEN)
            signal_ts = bcursor.advance(TIMESTAMP_STR_LEN)
            node_id = bcursor.advance(NODE_ID_LEN)

            node_register_id = bcursor.advance(NODE_REGISTER_ID_LEN)
            node_flag_field = bcursor.advance(NODE_FLAG_FIELD_LEN)

            sig_detail = {
                "signal_ts": signal_ts,
                "signal_id": signal_id,
                "node_id": node_id,
                "node_register_id": node_register_id,
                "node_flag_field": node_flag_field,
            }

            signals.append(sig_detail)

        return block_ts, pool_sign_key, signals


if __name__ == "__main__":
    sign_key = "abcdefge"

    db = leveldb.LevelDB("block")
    block = db.Get(sign_key.encode())

    bp = BlockParser()
    block_ts, pool_sign_key, signals = bp.parse(block)
    print(block_ts, pool_sign_key)

    for s in signals:
        print(s)
