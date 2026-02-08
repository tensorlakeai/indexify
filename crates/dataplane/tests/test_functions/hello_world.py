from tensorlake.applications import application, function


@application()
@function()
def hello_world(name: str) -> str:
    return f"Hello, {name}!"
