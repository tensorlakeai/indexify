# We send function inputs and outputs over gRPC.
# -1 means unlimited. We don't want to limit the size of data customers are using.
# The effective max message size in this case is about 1.9 GB, see the max payload test.
# This is due to internal hard gRPC limits. When we want to increase the message sizes
# we'll have to implement chunking for large messages.
_MAX_GRPC_MESSAGE_LENGTH = -1
# Disable port reuse: fail if multiple Function Executor Servers attempt to bind to the
# same port. This happens when Indexify users misconfigure the Servers. Disabling the port
# reuse results in a clear error message on Server startup instead of obscure errors later
# while Indexify cluster is serving tasks.
# If we don't disable port reuse then a random Server gets the requests so wrong tasks get
# routed to wrong servers.
_REUSE_SERVER_PORT = 0

GRPC_SERVER_OPTIONS = [
    ("grpc.max_receive_message_length", _MAX_GRPC_MESSAGE_LENGTH),
    ("grpc.max_send_message_length", _MAX_GRPC_MESSAGE_LENGTH),
    ("grpc.so_reuseport", _REUSE_SERVER_PORT),
]

GRPC_CHANNEL_OPTIONS = GRPC_SERVER_OPTIONS
