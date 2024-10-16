class Image:
    def __init__(self):
        self._image_name = None

        self._tag = "latest"

        self._base_image = "python:3.11.10-slim-bookworm"

        self._run_strs = []

    def name(self, image_name):
        self._image_name = image_name
        return self

    def tag(self, tag):
        self._tag = tag
        return self

    def base_image(self, base_image):
        self._base_image = base_image
        return self

    def run(self, run_str):
        self._run_strs.append(run_str)
        return self


DEFAULT_IMAGE_3_10 = (
    Image()
    .name("tensorlake/indexify-executor-default")
    .base_image("python:3.10.15-slim-bookworm")
    .tag("3.10")
    .run("pip install indexify")
)

DEFAULT_IMAGE_3_11 = (
    Image()
    .name("tensorlake/indexify-executor-default")
    .base_image("python:3.11.10-slim-bookworm")
    .tag("3.11")
    .run("pip install indexify")
)
