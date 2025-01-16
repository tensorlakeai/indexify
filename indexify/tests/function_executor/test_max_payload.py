import hashlib
import math
import os
import time
import unittest

from tensorlake import Graph, tensorlake_function
from tensorlake.functions_sdk.data_objects import File
from tensorlake.functions_sdk.object_serializer import CloudPickleSerializer
from testing import (
    FunctionExecutorServerTestCase,
    deserialized_function_output,
    run_task,
)

from indexify.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    InitializeResponse,
    RunTaskResponse,
    SerializedObject,
)
from indexify.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

# Current max input and output sizes that we support.
MAX_FUNCTION_PAYLOAD_SIZE_BYTES = math.floor(1.9 * 1024 * 1024 * 1024)  # 1.9 GB


def random_bytes(size: int) -> bytes:
    start_time = time.time()
    print(f"Generating random data of size {size} bytes...")
    random_data = os.urandom(size)
    end_time = time.time()
    print(f"Random data generation duration: {end_time - start_time} seconds")
    return random_data


def hash(data: bytes) -> str:
    start_time = time.time()
    print(f"Hashing data of size {len(data)} bytes...")
    hash_value = hashlib.sha256(data).hexdigest()
    end_time = time.time()
    print(f"Hashing duration: {end_time - start_time} seconds")
    return hash_value


@tensorlake_function()
def validate_max_input(input: File) -> str:
    if len(input.data) != MAX_FUNCTION_PAYLOAD_SIZE_BYTES:
        raise ValueError(
            f"Expected payload size to be {MAX_FUNCTION_PAYLOAD_SIZE_BYTES} bytes, but got {len(input.data)} bytes."
        )

    if hash(input.data) != input.sha_256:
        raise ValueError(
            "SHA-256 hash of the payload does not match the provided hash."
        )

    return "success"


@tensorlake_function()
def generate_max_output(x: int) -> File:
    data = random_bytes(MAX_FUNCTION_PAYLOAD_SIZE_BYTES)
    return File(data=data, sha_256=hash(data))


class TestMaxPayload(FunctionExecutorServerTestCase):
    def test_max_function_input_size(self):
        graph = Graph(
            name="test_max_function_input_size",
            description="test",
            start_node=validate_max_input,
        )
        max_input_data = random_bytes(MAX_FUNCTION_PAYLOAD_SIZE_BYTES)
        max_input = File(data=max_input_data, sha_256=hash(max_input_data))

        with self._rpc_channel() as channel:
            stub: FunctionExecutorStub = FunctionExecutorStub(channel)
            initialize_response: InitializeResponse = stub.initialize(
                InitializeRequest(
                    namespace="test",
                    graph_name="test",
                    graph_version="1",
                    function_name="validate_max_input",
                    graph=SerializedObject(
                        bytes=CloudPickleSerializer.serialize(
                            graph.serialize(additional_modules=[])
                        ),
                        content_type=CloudPickleSerializer.content_type,
                    ),
                )
            )
            self.assertTrue(initialize_response.success)

            run_task_response: RunTaskResponse = run_task(
                stub, function_name="validate_max_input", input=max_input
            )
            self.assertTrue(run_task_response.success)
            self.assertFalse(run_task_response.is_reducer)

            fn_outputs = deserialized_function_output(
                self, run_task_response.function_output
            )
            self.assertEqual(len(fn_outputs), 1)
            self.assertEqual("success", fn_outputs[0])

    def test_max_function_output_size(self):
        graph = Graph(
            name="test_max_function_output_size",
            description="test",
            start_node=generate_max_output,
        )

        with self._rpc_channel() as channel:
            stub: FunctionExecutorStub = FunctionExecutorStub(channel)
            initialize_response: InitializeResponse = stub.initialize(
                InitializeRequest(
                    namespace="test",
                    graph_name="test",
                    graph_version="1",
                    function_name="generate_max_output",
                    graph=SerializedObject(
                        bytes=CloudPickleSerializer.serialize(
                            graph.serialize(additional_modules=[])
                        ),
                        content_type=CloudPickleSerializer.content_type,
                    ),
                )
            )
            self.assertTrue(initialize_response.success)

            run_task_response: RunTaskResponse = run_task(
                stub, function_name="generate_max_output", input=1
            )

            self.assertTrue(run_task_response.success)
            self.assertFalse(run_task_response.is_reducer)

            fn_outputs = deserialized_function_output(
                self, run_task_response.function_output
            )
            self.assertEqual(len(fn_outputs), 1)
            output_file: File = fn_outputs[0]
            self.assertEqual(MAX_FUNCTION_PAYLOAD_SIZE_BYTES, len(output_file.data))
            self.assertEqual(hash(output_file.data), output_file.sha_256)


if __name__ == "__main__":
    unittest.main()
