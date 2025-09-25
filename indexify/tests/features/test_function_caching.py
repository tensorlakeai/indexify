import os
import tempfile
import unittest
from typing import List

import tensorlake.workflows.interface as tensorlake
from pydantic import BaseModel
from tensorlake.workflows.remote.deploy import deploy


class Payload(BaseModel):
    count: int
    generate_numbers_runs_file_path: str


# FIXME: Use default json serializer when dynamic function output serializer selection is supported.
@tensorlake.api(output_serializer="pickle")
@tensorlake.function()
def sum_numbers_api(payload: Payload) -> int:
    return sum_numbers(
        generate_numbers(payload.count, payload.generate_numbers_runs_file_path)
    )


@tensorlake.function()
def sum_numbers(numbers: List[int]) -> int:
    return sum(numbers)


@tensorlake.function(cacheable=True)
def generate_numbers(count: int, cache_file_path: str) -> List[int]:
    with open(cache_file_path, "a") as cache_file:
        cache_file.write("generate_numbers run\n")
    return [i + 2 for i in range(count)]


class TestFunctionCaching(unittest.TestCase):
    def setUp(self):
        deploy(__file__)

    def test_cacheable_function_runs_only_once(self):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.addCleanup(lambda: temp_file.close() or os.unlink(temp_file.name))
        payload = Payload(count=5, generate_numbers_runs_file_path=temp_file.name)

        # First request should run the function and add a line to the file
        request_1: tensorlake.Request = tensorlake.call_remote_api(
            sum_numbers_api, payload
        )
        self.assertEqual(request_1.output(), sum([2, 3, 4, 5, 6]))

        with open(temp_file.name, "r") as f:
            runs: List[str] = f.readlines()
        self.assertEqual(len(runs), 1)  # generate_numbers function should have run once
        self.assertEqual(runs[0], "generate_numbers run\n")

        # Second request should use the cache and not run the function again
        request_2: tensorlake.Request = tensorlake.call_remote_api(
            sum_numbers_api, payload
        )
        self.assertEqual(request_2.output(), sum([2, 3, 4, 5, 6]))

        with open(temp_file.name, "r") as f:
            runs: List[str] = f.readlines()
        self.assertEqual(
            len(runs), 1
        )  # generate_numbers function should still have run only once
        self.assertEqual(runs[0], "generate_numbers run\n")


if __name__ == "__main__":
    unittest.main()
