from blockparser import BlockParser
from blockstorageserver import BlockStorageClient
from utils import BufferCursor
import fortunate_system_const as FC

from blockparser import BlockParser
from functools import reduce

from utils import get_logger



class BlockVerifier:

    def __init__(self, *args, **kwargs):
        self.ssclient = BlockStorageClient.get_client("verifier")
        self.logger = get_logger("verifier")

    def signals_op(self, signals, flag_index, op):
        _f = self.get_flag_op(flag_index)
        flags = map(lambda x: _f(x) , signals)
        return reduce(op, flags)


    def verify_block(self, sign_key):
        block_count, buffer = self.ssclient.api.get_blockbuffer_finalized(sign_key)
        records = BlockParser.parse(buffer.encode('utf8'))

        event_records = [*filter(lambda x: x["record_type"] == "01", records)]
        events = { x["event_hash"] : x for x in event_records }

        for event_hash in events:
            event = events[event_hash]

            self.verify_event(event_hash, event, records)


    def verify_event(self, event_hash, event, records):
        ref_signals = event["signals"]

        self.logger.info(f"Verify event#{event_hash}: {event}")
        signal_records = [*filter(lambda x: x["record_type"] == "02" and x["signal_id"] in ref_signals, records)]
        signals = { x["signal_id"]: x for x in signal_records }

        flag = self.signals_op(signal_records, 0, lambda y,x: int(x) * int(y))

        assert str(event["state"]) == str(flag)

        self.logger.info(f"event#{event_hash} verified.")
        return True
    
    def get_flag_op(self, findex):

        def op(signal):
            flag = signal["flag_field"][findex]

            self.logger.debug(f"flag: {flag} from signal : {signal}")
            return flag

        return op





