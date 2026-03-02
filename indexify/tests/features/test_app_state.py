import unittest

from tensorlake.applications import (
    Request,
    RequestContext,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def app_state_set_and_get(value: int) -> int:
    """Set a value in app_state and read it back in the same function."""
    ctx: RequestContext = RequestContext.get()
    ctx.app_state.set("key1", value)
    return ctx.app_state.get("key1")


@application()
@function()
def app_state_set_then_read_downstream(value: int) -> int:
    """Set a value in app_state and read it in a downstream function."""
    ctx: RequestContext = RequestContext.get()
    ctx.app_state.set("downstream_key", value)
    return app_state_read_downstream()


@function()
def app_state_read_downstream() -> int:
    ctx: RequestContext = RequestContext.get()
    return ctx.app_state.get("downstream_key")


@application()
@function()
def app_state_kv(mode: str, key: str = "", value: int = 0) -> int:
    """Single app that can write or read any key in app_state.

    mode="write": sets key to value, returns value.
    mode="read": reads key, returns its value.
    """
    ctx: RequestContext = RequestContext.get()
    if mode == "write":
        ctx.app_state.set(key, value)
        return value
    else:
        return ctx.app_state.get(key)


@application()
@function()
def app_state_get_default(default: str) -> str:
    """Get a non-existent key with a default value."""
    ctx: RequestContext = RequestContext.get()
    return ctx.app_state.get("non_existing_key", default)


@application()
@function()
def app_state_get_none() -> None:
    """Get a non-existent key without a default should return None."""
    ctx: RequestContext = RequestContext.get()
    return ctx.app_state.get("non_existing_key")


class TestAppState(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def test_set_and_get_simple_value(self):
        """Set and get a value within the same function execution."""
        request: Request = run_remote_application(app_state_set_and_get, 42)
        self.assertEqual(request.output(), 42)

    def test_set_then_read_in_downstream_function(self):
        """Set a value in one function and read it in a downstream function of the same request."""
        request: Request = run_remote_application(
            app_state_set_then_read_downstream, 99
        )
        self.assertEqual(request.output(), 99)

    def test_two_requests_read_write_each_others_data(self):
        """Two different requests can read each other's data via app_state."""
        # Request 1: writes key "a" = 100
        r1: Request = run_remote_application(app_state_kv, "write", "a", 100)
        self.assertEqual(r1.output(), 100)

        # Request 2: writes key "b" = 200
        r2: Request = run_remote_application(app_state_kv, "write", "b", 200)
        self.assertEqual(r2.output(), 200)

        # Request 3: reads key "a" (written by request 1)
        r3: Request = run_remote_application(app_state_kv, "read", "a")
        self.assertEqual(r3.output(), 100)

        # Request 4: reads key "b" (written by request 2)
        r4: Request = run_remote_application(app_state_kv, "read", "b")
        self.assertEqual(r4.output(), 200)

    def test_later_request_overwrites_earlier(self):
        """Last writer wins: a later request can overwrite an earlier request's value."""
        # Request 1: writes key "counter" = 10
        r1: Request = run_remote_application(app_state_kv, "write", "counter", 10)
        self.assertEqual(r1.output(), 10)

        # Request 2: overwrites key "counter" = 20
        r2: Request = run_remote_application(app_state_kv, "write", "counter", 20)
        self.assertEqual(r2.output(), 20)

        # Request 3: reads the latest value
        r3: Request = run_remote_application(app_state_kv, "read", "counter")
        self.assertEqual(r3.output(), 20)

    def test_get_default_value(self):
        """Getting a non-existent key returns the provided default."""
        request: Request = run_remote_application(app_state_get_default, "fallback")
        self.assertEqual(request.output(), "fallback")

    def test_get_without_default_returns_none(self):
        """Getting a non-existent key without a default returns None."""
        request: Request = run_remote_application(app_state_get_none)
        self.assertIsNone(request.output())


if __name__ == "__main__":
    unittest.main()
