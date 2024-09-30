import unittest
from typing import List, Union

from pydantic import BaseModel

from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import (
    indexify_function,
    indexify_router,
)


class TestValidations(unittest.TestCase):
    def test_function_signature_types(self):
        class ComplexType(BaseModel):
            pass

        @indexify_function()
        def node1(a: int, b: ComplexType) -> int:
            pass

        @indexify_function()
        def node2(b):
            pass

        g = Graph(
            "test-graph",
            start_node=node1,
        )

        msg = "Input param b in node2 has empty type annotation"

        with self.assertRaises(Exception) as cm:
            g.add_edge(node1, node2)

        self.assertEqual(msg, str(cm.exception))

    def test_function_return_type_annotation(self):
        class ComplexType(BaseModel):
            pass

        @indexify_function()
        def node1(a: int, b: ComplexType) -> int:
            pass

        @indexify_function()
        def node2(b: float):
            pass

        g = Graph(
            "test-graph",
            start_node=node1,
        )

        msg = "Function node2 has empty return type annotation"

        with self.assertRaises(Exception) as cm:
            g.add_edge(node1, node2)

        self.assertEqual(msg, str(cm.exception))

    def test_callables_are_in_added_nodes(self):
        class ComplexType(BaseModel):
            pass

        def node1(a: int, b: ComplexType) -> int:
            pass

        @indexify_function()
        def node2(b: int) -> ComplexType:
            pass

        with self.assertRaises(Exception) as cm:
            g = Graph(
                "test-graph",
                start_node=node1,
            )

            g.add_edge(node1, node2)

        msg = "Unable to add node of type `<class 'function'>`. Required, `IndexifyFunction` or `IndexifyRouter`"
        self.assertEqual(msg, str(cm.exception))

    def test_router_callables_are_in_added_nodes_union(self):
        @indexify_function()
        def node0(a: int) -> int:
            pass

        @indexify_function()
        def node1(a: int) -> int:
            pass

        @indexify_function()
        def node2(a: int) -> int:
            pass

        @indexify_function()
        def node3(a: int) -> int:
            pass

        @indexify_router()
        def router(a: int) -> List[Union[node1, node3]]:
            pass

        @indexify_router()
        def router2(a: int) -> Union[node1, node3]:
            pass

        with self.assertRaises(Exception) as cm:
            g = Graph(
                "test-graph",
                start_node=node0,
            )

            g.add_edge(node0, router)
            g.route(router, [node1, node2])
        msg = "Unable to find node3 in to_nodes ['node1', 'node2']"
        self.assertEqual(msg, str(cm.exception))
        
        with self.assertRaises(Exception) as cm:
            g = Graph(
                "test-graph",
                start_node=node0,
            )

            g.add_edge(node0, router)
            g.route(router2, [node1, node2])
        msg = "Unable to find node3 in to_nodes ['node1', 'node2']"
        self.assertEqual(msg, str(cm.exception))




if __name__ == "__main__":
    unittest.main()
