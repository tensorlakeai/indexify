import unittest
from typing import Generator

import grpc

from indexify.proto.executor_api_pb2 import (
    DesiredExecutorState,
    ExecutorState,
    GetDesiredExecutorStatesRequest,
    ReportExecutorStateRequest,
    ReportExecutorStateResponse,
)
from indexify.proto.executor_api_pb2_grpc import ExecutorAPIStub


class TestServerExecutorGRPCAPI(unittest.TestCase):
    def test_report_executor_state_returns_a_response(self):
        with grpc.insecure_channel("localhost:8901") as channel:
            channel: grpc.Channel
            stub = ExecutorAPIStub(channel)
            response: ReportExecutorStateResponse = stub.report_executor_state(
                ReportExecutorStateRequest(
                    executor_state=ExecutorState(
                        executor_id="test-executor-id",
                    ),
                ),
                timeout=5,
            )
            print("Received report Executor state response from the Server.")
            print(response)

    def test_get_desired_executor_states_returns_a_response(self):
        with grpc.insecure_channel("localhost:8901") as channel:
            channel: grpc.Channel
            stub = ExecutorAPIStub(channel)
            desired_states_stream: Generator[DesiredExecutorState, None, None] = (
                stub.get_desired_executor_states(
                    GetDesiredExecutorStatesRequest(
                        executor_id="test-executor-id",
                    ),
                    timeout=5,
                )
            )
            print("Waiting until a desired Executor state message is received...")
            for desired_state in desired_states_stream:
                print("Received a desired Executor state message.")
                print(desired_state)
                break


if __name__ == "__main__":
    unittest.main()
