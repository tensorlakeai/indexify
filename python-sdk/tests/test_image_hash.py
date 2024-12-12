

import unittest

from indexify.functions_sdk.image import Image, BASE_IMAGE_NAME


KNOWN_GOOD_HASH = "229514da1c19e40fda77e8b4a4990f69ce1ec460f025f4e1367bb2219f6abea1"

class TestImage(unittest.TestCase):
    def test_image_hash(self):
        i = Image().name("test").base_image("static_base_image").run("pip install all_the_things").tag("test")
        i._sdk_version = "1.2.3" # This needs to be statc for the hash to be predictable
        
        self.assertEqual(i.hash(), KNOWN_GOOD_HASH)

if __name__ == "__main__":
    unittest.main()
