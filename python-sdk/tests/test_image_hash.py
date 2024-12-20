import unittest
import os
import shutil

from indexify.functions_sdk.image import BASE_IMAGE_NAME, Image

KNOWN_GOOD_HASH = "3f25487a14c4c0e8c9948545e85e17404717a3eebf33c91821b6d8d06cc6d704"


class TestImage(unittest.TestCase):
    def setUp(self):
        os.mkdir("copy_test_dir")
        with open("copy_test_dir/test_file", "w") as fp:
            fp.write("Test data\n")
    
    def tearDown(self):
        shutil.rmtree("copy_test_dir")

    def test_copy_hash_update(self):
        i = self.newTestImage()

        prevHash = i.hash()

        with open("copy_test_dir/test_file", "w+") as fp:
            fp.write("Some more test data\n")

        self.assertNotEqual(i.hash(), prevHash)

    def test_image_hash(self):
        i = (
            Image()
            .name("test")
            .base_image("static_base_image")
            .copy("copy_test_dir", "test_dir")
            .run("pip install all_the_things")
            .tag("test")
        )
        i._sdk_version = (
            "1.2.3"  # This needs to be static for the hash to be predictable
        )

        self.assertEqual(i.hash(), KNOWN_GOOD_HASH)

    def newTestImage(self):
        i = (
            Image()
            .name("test")
            .base_image("static_base_image")
            .copy("copy_test_dir", "test_dir")
            .run("pip install all_the_things")
            .tag("test")
        )
        i._sdk_version = (
            "1.2.3"  # This needs to be static for the hash to be predictable
        )
        return i

if __name__ == "__main__":
    unittest.main()
