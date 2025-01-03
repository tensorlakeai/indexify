import os
import tempfile
import unittest
from typing import Optional

from parameterized import parameterized

from indexify import Graph, indexify_function
from tests.testing import remote_or_local_graph

cached_file_descriptor: Optional[int] = None


@indexify_function()
def write_to_cacheable_fd_1(file_path: str) -> str:
    global cached_file_descriptor

    if cached_file_descriptor is None:
        cached_file_descriptor = os.open(file_path, os.O_WRONLY | os.O_TRUNC)
    os.write(cached_file_descriptor, "write_to_cacheable_fd_1\n".encode())

    return "success"


class TestFileDescriptorCaching(unittest.TestCase):
    @parameterized.expand([(False), (True)])
    def test_second_write_goes_to_cached_file_descriptor_if_same_func(self, is_remote):
        global cached_file_descriptor
        # Make sure that this test executed in local graph mode doesn't affect the remote graph mode test.
        cached_file_descriptor = None

        with tempfile.NamedTemporaryFile("rb") as file:
            graph = Graph(
                name="test_file_descriptor_caching",
                description="test",
                start_node=write_to_cacheable_fd_1,
            )
            graph = remote_or_local_graph(graph, is_remote)

            first_invocation_id = graph.run(
                block_until_done=True, file_path=file.name
            )  # file.name is actually the path
            output = graph.output(first_invocation_id, "write_to_cacheable_fd_1")
            self.assertEqual(output, ["success"])

            second_invocation_id = graph.run(
                block_until_done=True, file_path=file.name
            )  # file.name is actually the path
            output = graph.output(second_invocation_id, "write_to_cacheable_fd_1")
            self.assertEqual(output, ["success"])

            # If the file descriptor is cached between runs of write_to_cacheable_fd_1 then the file
            # will contain two lines, otherwise it'll contain only one line because the second write
            # will create a new file descriptor and overwrite the first write.
            self.assertEqual(
                file.read(),
                "write_to_cacheable_fd_1\nwrite_to_cacheable_fd_1\n".encode(),
            )


if __name__ == "__main__":
    unittest.main()
