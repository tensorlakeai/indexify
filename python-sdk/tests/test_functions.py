import unittest
from typing import List, Union

from pydantic import BaseModel

from indexify.functions_sdk.indexify_functions import (
    GraphInvocationContext,
    IndexifyFunctionWrapper,
    get_ctx,
    indexify_function,
    indexify_router,
)

TEST_GRAPH_CTX = GraphInvocationContext(
    invocation_id="123", graph_name="test", graph_version="1"
)


class TestFunctionWrapper(unittest.TestCase):
    def test_basic_features(self):
        @indexify_function()
        def extractor_a(url: str) -> str:
            """
            Random description of extractor_a
            """
            return "hello"

        extractor_wrapper = IndexifyFunctionWrapper(extractor_a, TEST_GRAPH_CTX)
        result, err = extractor_wrapper.run_fn({"url": "foo"})
        self.assertEqual(result[0], "hello")

    def test_get_output_model(self):
        @indexify_function()
        def extractor_b(url: str) -> str:
            """
            Random description of extractor_b
            """
            return "hello"

        extractor_wrapper = IndexifyFunctionWrapper(extractor_b, TEST_GRAPH_CTX)
        result = extractor_wrapper.get_output_model()
        self.assertEqual(result, str)

    def test_list_output_model(self):
        @indexify_function()
        def extractor_b(url: str) -> List[str]:
            """
            Random description of extractor_b
            """
            return ["hello", "world"]

        extractor_wrapper = IndexifyFunctionWrapper(extractor_b, TEST_GRAPH_CTX)
        result = extractor_wrapper.get_output_model()
        self.assertEqual(result, str)

    def test_router_fn(self):
        @indexify_function()
        def func_a(x: int) -> int:
            return 6

        @indexify_function()
        def func_b(x: int) -> int:
            return 7

        @indexify_router()
        def router_fn(url: str) -> List[Union[func_a, func_b]]:
            """
            Random description of router_fn
            """
            return [func_a]

        router_wrapper = IndexifyFunctionWrapper(router_fn, TEST_GRAPH_CTX)
        result, err = router_wrapper.run_router({"url": "foo"})
        self.assertEqual(result, ["func_a"])

    def test_accumulate(self):
        class AccumulatedState(BaseModel):
            x: int

        @indexify_function(accumulate=AccumulatedState)
        def accumulate_fn(acc: AccumulatedState, x: int) -> AccumulatedState:
            acc.x += x
            return acc

        wrapper = IndexifyFunctionWrapper(accumulate_fn, TEST_GRAPH_CTX)
        result, err = wrapper.run_fn(acc=AccumulatedState(x=12), input={"x": 1})
        self.assertEqual(result[0].x, 13)

    def test_get_ctx(self):
        @indexify_function()
        def extractor_c(url: str) -> str:
            ctx = get_ctx()  # type: ignore
            ctx.set_state_key("foo", "bar")
            foo_val = ctx.get_state_key("foo")
            return ctx.invocation_id

        extractor_wrapper = IndexifyFunctionWrapper(extractor_c, TEST_GRAPH_CTX)
        result, _ = extractor_wrapper.run_fn({"url": "foo"})
        self.assertEqual(result[0], "123")

    # FIXME: Partial extractor is not working
    # def test_partial_extractor(self):
    #    @extractor()
    #    def extractor_c(url: str, some_other_param: str) -> str:
    #        """
    #        Random description of extractor_c
    #        """
    #        return f"hello {some_other_param}"

    #    print(type(extractor_c))
    #    partial_extractor = extractor_c.partial(some_other_param="world")
    #    result = partial_extractor.extract(BaseData.from_data(url="foo"))
    #    self.assertEqual(result[0].payload, "hello world")


if __name__ == "__main__":
    unittest.main()
