# We send function inputs and outputs over gRPC.
# -1 means unlimited. We don't want to limit the size of data customers are using.
# The effective max message size in this case is about 1.9 GB, see the max payload test.
# This is due to internal hard gRPC limits. When we want to increase the message sizes
# we'll have to implement chunking for large messages.
_MAX_GRPC_MESSAGE_LENGTH = -1

GRPC_SERVER_OPTIONS = [
    ("grpc.max_receive_message_length", _MAX_GRPC_MESSAGE_LENGTH),
    ("grpc.max_send_message_length", _MAX_GRPC_MESSAGE_LENGTH),
]

GRPC_CHANNEL_OPTIONS = GRPC_SERVER_OPTIONS
