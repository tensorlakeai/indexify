import subprocess
import unittest

from pydantic import BaseModel
from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


class RunCommandPayload(BaseModel):
    command: str
    need_output: bool
    check_exit_code: bool


@application()
@function()
def run_command(payload: RunCommandPayload) -> str:
    try:
        print(f"Running command: {payload.command}")
        result = subprocess.run(
            payload.command,
            shell=True,
            check=payload.check_exit_code,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if payload.need_output:
            return result.stdout
        else:
            return "success"
    except subprocess.CalledProcessError as e:
        raise Exception(
            f"Command '{payload.command}' failed with error: {e.stderr.strip()}"
        ) from e


class TestPublicInternetConnectivity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def test_connect_to_public_https_endpoint(self):
        request: Request = run_remote_application(
            run_command,
            RunCommandPayload(
                command="apt-get update && apt-get install -y curl",
                need_output=False,
                check_exit_code=True,
            ),
        )
        self.assertEqual(request.output(), "success")

        request: Request = run_remote_application(
            run_command,
            RunCommandPayload(
                command="curl --silent --output /dev/null --write-out '%{http_code}' https://example.com",
                need_output=True,
                check_exit_code=True,
            ),
        )
        http_code = request.output().strip()
        self.assertEqual(http_code, "200")

    def test_ip_is_hidden_behind_nat(self):
        request: Request = run_remote_application(
            run_command,
            RunCommandPayload(
                command="apt-get update && apt-get install -y curl",
                need_output=False,
                check_exit_code=True,
            ),
        )
        self.assertEqual(request.output(), "success")

        request: Request = run_remote_application(
            run_command,
            RunCommandPayload(
                command="curl --silent https://api.ipify.org",
                need_output=True,
                check_exit_code=True,
            ),
        )
        public_ip = request.output().strip()
        # Ensure the public IP is a valid IPv4 address
        self.assertRegex(public_ip, r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")

        request: Request = run_remote_application(
            run_command,
            RunCommandPayload(
                command="hostname --all-ip-addresses",
                need_output=True,
                check_exit_code=True,
            ),
        )
        ip_address = request.output().strip()
        self.assertNotEqual(public_ip, ip_address)


if __name__ == "__main__":
    unittest.main()
