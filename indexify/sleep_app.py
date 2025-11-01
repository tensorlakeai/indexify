import time

from tensorlake.applications import application, function


@application()
@function()
def sleep_function(x: int) -> str:
    time.sleep(x)
    return "Slept for 10 seconds"


if __name__ == "__main__":
    from tensorlake.applications import run_remote_application
    from tensorlake.applications.remote.deploy import deploy_applications

    deploy_applications(__file__)
    request = run_remote_application(sleep_function, 10)
    print(request.output())
