from indexify.functions_sdk.data_objects import BaseData
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function
from indexify.utils import Image

base_image = "python:3.9-slim"
pip_str = "RUN pip install requests"
image_name = "func1-image-1"


class FilteredData(BaseData):
    output: str


class GenerateData(BaseData):
    output: str


image_func_1 = Image().image_name(image_name).base_image(base_image).run(pip_str)


@indexify_function(image=image_func_1)
def generate_data(some_input: str) -> GenerateData:
    """
    Duplicate the input
    """
    return GenerateData(output=",".join(2 * [some_input]))


tag = "func2-image-1"
image_func_2 = (
    Image().image_name(image_name).tag(tag).base_image(base_image).run(pip_str)
)


@indexify_function(image=image_func_2)
def filter_bad(some_input: GenerateData) -> FilteredData:
    """
    Duplicate the input
    """
    return FilteredData(output=",".join([i for i in some_input.output if "good" in i]))


if __name__ == "__main__":
    # make sure that the workflow has this entry point. The cli has to pick up
    # stuff from here.
    g = Graph(
        "test-graph",
        start_node=generate_data,
    )

    # Parse the PDF which was downloaded
    g.add_edge(generate_data, filter_bad)

    # Use the script command,
    # `indexify-cli indexify-python-sdk/examples/graph_image_test/workflow.py`
    # or `indexify-cli indexify-python-sdk/examples/graph_image_test/workflow.py generate_data`
    # or in the code,
    # generate_data.image_builder.build_image()
