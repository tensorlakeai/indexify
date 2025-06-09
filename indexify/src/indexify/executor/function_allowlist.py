from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class FunctionURI:
    namespace: str
    compute_graph: str
    compute_fn: str
    version: Optional[str] = None


def function_allowlist_to_indexed_dict(
    function_allowlist: List[FunctionURI],
) -> Dict[str, str]:
    """Returns a dictionary with each function URI in the allowlist as a key-value pair.

    The keys are prefixed indexes in function allowlist, and the values are the function URIs
    """
    indexed_dict = {}
    counter = 0
    for function_uri in function_allowlist:
        function_uri: FunctionURI
        indexed_dict[f"function_allowlist_{counter}"] = ":".join(
            [
                function_uri.namespace,
                function_uri.compute_graph,
                function_uri.compute_fn,
                str(function_uri.version),
            ]
        )
        counter += 1
    return indexed_dict


def parse_function_uris(function_uri_strs: List[str]) -> List[FunctionURI]:
    """Parses a list of function URIs from strings to FunctionURI objects."""
    uris: List[FunctionURI] = []
    for uri_str in function_uri_strs:
        tokens = uri_str.split(":")
        if len(tokens) < 3 or len(tokens) > 4:
            raise ValueError(
                "Function should be specified as <namespace>:<workflow>:<function>:<version> or"
                "<namespace>:<workflow>:<function>"
            )
        version: Optional[str] = None
        if len(tokens) == 4:
            version = tokens[3]

        uris.append(
            FunctionURI(
                namespace=tokens[0],
                compute_graph=tokens[1],
                compute_fn=tokens[2],
                version=version,
            )
        )

    return uris
