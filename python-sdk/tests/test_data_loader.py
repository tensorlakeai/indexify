import os
import shutil
import tempfile
import unittest

from indexify.data_loaders import LocalDirectoryLoader


class TestLocalDirectoryLoader(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

        # Create some test files
        self.test_files = ["test1.txt", "test2.md", "test3.txt", "test4.py"]
        for file_name in self.test_files:
            with open(os.path.join(self.test_dir, file_name), "w") as f:
                f.write("This is a test file.")

    def tearDown(self):
        # Remove the temporary directory after the test
        shutil.rmtree(self.test_dir)

    def test_load_with_extensions(self):
        loader = LocalDirectoryLoader(self.test_dir, file_extensions=[".txt"])
        file_metadata_list = loader.load()

        # Check that only .txt files are loaded
        self.assertEqual(len(file_metadata_list), 2)
        loaded_files = [metadata.path for metadata in file_metadata_list]
        self.assertIn(os.path.join(self.test_dir, "test1.txt"), loaded_files)
        self.assertIn(os.path.join(self.test_dir, "test3.txt"), loaded_files)

    def test_load_without_extensions(self):
        loader = LocalDirectoryLoader(self.test_dir)
        file_metadata_list = loader.load()

        # Check that all files are loaded
        self.assertEqual(len(file_metadata_list), 4)
        loaded_files = [metadata.path for metadata in file_metadata_list]
        for file_name in self.test_files:
            self.assertIn(os.path.join(self.test_dir, file_name), loaded_files)

    def test_state(self):
        loader = LocalDirectoryLoader(self.test_dir, file_extensions=[".txt"])
        loader.load()
        state = loader.state()

        # Check that the state tracks processed files correctly
        self.assertEqual(len(state["processed_files"]), 2)
        self.assertIn(
            os.path.join(self.test_dir, "test1.txt"), state["processed_files"]
        )
        self.assertIn(
            os.path.join(self.test_dir, "test3.txt"), state["processed_files"]
        )


if __name__ == "__main__":
    unittest.main()
