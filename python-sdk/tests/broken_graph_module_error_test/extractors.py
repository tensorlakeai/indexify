import sys
import unittest

from first_p_dep import return_x

from indexify import RemoteGraph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function


@indexify_function()
def extractor_a(a: int) -> int:
    """
    Do stuff.
    """
    print("Running executor")
    return return_x(x=a)


@indexify_function()
def extractor_c(s: str) -> str:
    """
    Do nothing, just return.
    """
    return "this is a return from extractor_c"
