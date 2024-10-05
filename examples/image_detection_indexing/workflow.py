from indexify import indexify_router
from indexify.functions_sdk.indexify_functions import IndexifyFunction

from indexify.functions_sdk.data_objects import File
from typing import List

class DetectObjects(IndexifyFunction):
    def __init__(self):
        pass

    def run(self, f: File) -> List:
        pass
@indexify_router()
def route_images_to_downstream_functions(f: File) ->  List[]:
    pass