from indexify.executor.api_objects import Task

tls_config = {
    "use_tls": True,
    "tls_config": {
        "ca_bundle_path": "/path/to/ca_bundle.pem",
        "cert_path": "/path/to/cert.pem",
        "key_path": "/path/to/key.pem"
    }
}

cert_path = tls_config["tls_config"]["cert_path"]
key_path = tls_config["tls_config"]["key_path"]
ca_bundle_path = tls_config["tls_config"]["ca_bundle_path"]
service_url = "localhost:8900"
config_path = "test/config/path"
code_path = "test/code_path"
task = Task(
    id = "test_id",
    namespace = "default",
    compute_graph = "test_compute_graph",
    compute_fn = "test_compute_fn",
    invocation_id = "test_invocation_id",
    input_key = "test|input|key",
    requester_output_id = "test_output_id",
    graph_version = 1,
)
