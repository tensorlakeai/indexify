# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import warnings

import grpc

from indexify.proto import executor_api_pb2 as indexify_dot_proto_dot_executor__api__pb2

GRPC_GENERATED_VERSION = "1.74.0"
GRPC_VERSION = grpc.__version__
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower

    _version_not_supported = first_version_is_lower(
        GRPC_VERSION, GRPC_GENERATED_VERSION
    )
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    raise RuntimeError(
        f"The grpc package installed is at version {GRPC_VERSION},"
        + f" but the generated code in indexify/proto/executor_api_pb2_grpc.py depends on"
        + f" grpcio>={GRPC_GENERATED_VERSION}."
        + f" Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}"
        + f" or downgrade your generated code using grpcio-tools<={GRPC_VERSION}."
    )


class ExecutorAPIStub(object):
    """Internal API for scheduling and running tasks on Executors. Executors are acting as clients of this API.
    Server is responsible for scheduling tasks on Executors and Executors are responsible for running the tasks.

    Rename with caution. Existing clients won't find the service if the service name changes. A HTTP2 ingress proxy
    might use the service name in it HTTP2 path based routing rules. See how gRPC uses service names in its HTTP2 paths
    at https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.report_executor_state = channel.unary_unary(
            "/executor_api_pb.ExecutorAPI/report_executor_state",
            request_serializer=indexify_dot_proto_dot_executor__api__pb2.ReportExecutorStateRequest.SerializeToString,
            response_deserializer=indexify_dot_proto_dot_executor__api__pb2.ReportExecutorStateResponse.FromString,
            _registered_method=True,
        )
        self.get_desired_executor_states = channel.unary_stream(
            "/executor_api_pb.ExecutorAPI/get_desired_executor_states",
            request_serializer=indexify_dot_proto_dot_executor__api__pb2.GetDesiredExecutorStatesRequest.SerializeToString,
            response_deserializer=indexify_dot_proto_dot_executor__api__pb2.DesiredExecutorState.FromString,
            _registered_method=True,
        )


class ExecutorAPIServicer(object):
    """Internal API for scheduling and running tasks on Executors. Executors are acting as clients of this API.
    Server is responsible for scheduling tasks on Executors and Executors are responsible for running the tasks.

    Rename with caution. Existing clients won't find the service if the service name changes. A HTTP2 ingress proxy
    might use the service name in it HTTP2 path based routing rules. See how gRPC uses service names in its HTTP2 paths
    at https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md.
    """

    def report_executor_state(self, request, context):
        """Called by Executor every 5 seconds to report that it's still alive and provide its current state.

        Missing 3 reports will result in the Executor being deregistered by Server.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def get_desired_executor_states(self, request, context):
        """Called by Executor to open a stream of its desired states. When Server wants Executor to change something
        it puts a message on the stream with the new desired state of the Executor.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_ExecutorAPIServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "report_executor_state": grpc.unary_unary_rpc_method_handler(
            servicer.report_executor_state,
            request_deserializer=indexify_dot_proto_dot_executor__api__pb2.ReportExecutorStateRequest.FromString,
            response_serializer=indexify_dot_proto_dot_executor__api__pb2.ReportExecutorStateResponse.SerializeToString,
        ),
        "get_desired_executor_states": grpc.unary_stream_rpc_method_handler(
            servicer.get_desired_executor_states,
            request_deserializer=indexify_dot_proto_dot_executor__api__pb2.GetDesiredExecutorStatesRequest.FromString,
            response_serializer=indexify_dot_proto_dot_executor__api__pb2.DesiredExecutorState.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "executor_api_pb.ExecutorAPI", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers(
        "executor_api_pb.ExecutorAPI", rpc_method_handlers
    )


# This class is part of an EXPERIMENTAL API.
class ExecutorAPI(object):
    """Internal API for scheduling and running tasks on Executors. Executors are acting as clients of this API.
    Server is responsible for scheduling tasks on Executors and Executors are responsible for running the tasks.

    Rename with caution. Existing clients won't find the service if the service name changes. A HTTP2 ingress proxy
    might use the service name in it HTTP2 path based routing rules. See how gRPC uses service names in its HTTP2 paths
    at https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md.
    """

    @staticmethod
    def report_executor_state(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/executor_api_pb.ExecutorAPI/report_executor_state",
            indexify_dot_proto_dot_executor__api__pb2.ReportExecutorStateRequest.SerializeToString,
            indexify_dot_proto_dot_executor__api__pb2.ReportExecutorStateResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True,
        )

    @staticmethod
    def get_desired_executor_states(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            "/executor_api_pb.ExecutorAPI/get_desired_executor_states",
            indexify_dot_proto_dot_executor__api__pb2.GetDesiredExecutorStatesRequest.SerializeToString,
            indexify_dot_proto_dot_executor__api__pb2.DesiredExecutorState.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True,
        )
