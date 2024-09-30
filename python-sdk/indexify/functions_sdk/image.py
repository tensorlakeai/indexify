class Image:
    def __init__(self):
        self._image_name = None

        self._tag = "latest"

        self._base_image = None

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


DEFAULT_IMAGE = (
    Image()
    .name("indexify-executor-default")
    .base_image("python:3.10.15-slim-bookworm")
    .tag("latest")
    .run("pip install indexify")
)
