from blockparser import BlockParser


class Verifier:

    def __init__(self, *args, **kwargs):
        self.ssclient = BlockStorageClient.get_client("verifier")

    def verify_event(self, sign_key, event_hash):
        block_buffer = self.ssclient.api.request_get_block_finalized(sign_key)
        print(block_buffer)


if __name__ == "__main__":
    pass



