from typing import Dict, List, Optional

from ..api_objects import FunctionURI


def function_allowlist_to_info_dict(
    function_allowlist: Optional[List[FunctionURI]],
) -> Dict[str, str]:
    if function_allowlist is None:
        return {"function_allowlist": "None"}

    info = {}
    counter = 0
    for function_uri in function_allowlist:
        function_uri: FunctionURI
        info[f"function_allowlist_{counter}"] = ":".join(
            [
                function_uri.namespace,
                function_uri.compute_graph,
                function_uri.compute_fn,
                str(function_uri.version),
            ]
        )
        counter += 1
    return info
