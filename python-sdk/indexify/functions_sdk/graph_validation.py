import inspect
import re
from typing import List, Type, Union

from .indexify_functions import IndexifyFunction, IndexifyRouter


def validate_node(indexify_fn: Union[Type[IndexifyFunction], Type[IndexifyRouter]]):
    if inspect.isfunction(indexify_fn):
        raise Exception(
            f"Unable to add node of type `{type(indexify_fn)}`. "
            f"Required, `IndexifyFunction` or `IndexifyRouter`"
        )
    if not (
        issubclass(indexify_fn, IndexifyFunction)
        or issubclass(indexify_fn, IndexifyRouter)
    ):
        raise Exception(
            f"Unable to add node of type `{indexify_fn.__name__}`. "
            f"Required, `IndexifyFunction` or `IndexifyRouter`"
        )

    signature = inspect.signature(indexify_fn.run)

    for param in signature.parameters.values():
        if param.name == "self":
            continue
        if param.annotation == inspect.Parameter.empty:
            raise Exception(
                f"Input param {param.name} in {indexify_fn.name} has empty"
                f" type annotation"
            )

    if signature.return_annotation == inspect.Signature.empty:
        raise Exception(f"Function {indexify_fn.name} has empty return type annotation")


def validate_route(
    from_node: Type[IndexifyRouter], to_nodes: List[Type[IndexifyFunction]]
):
    signature = inspect.signature(from_node.run)

    if signature.return_annotation == inspect.Signature.empty:
        raise Exception(f"Function {from_node.name} has empty return type annotation")

    # We lose the exact type string when the object is created
    source = inspect.getsource(from_node.run)

    union_pattern = r"Union\[((?:\w+(?:,\s*)?)+)\]"
    union_match = re.search(union_pattern, source)

    src_route_nodes = None
    if union_match:
        # nodes = re.findall(r'\w+', match.group(1))
        src_route_nodes = [node.strip() for node in union_match.group(1).split(",")]
        if len(src_route_nodes) <= 1:
            raise Exception(f"Invalid router for {from_node.name}, lte 1 route.")
    else:
        raise Exception(
            f"Invalid router for {from_node.name}, cannot find output nodes"
        )

    to_node_names = [i.name for i in to_nodes]

    for src_node in src_route_nodes:
        if src_node not in to_node_names:
            raise Exception(
                f"Unable to find {src_node} in to_nodes " f"{to_node_names}"
            )
