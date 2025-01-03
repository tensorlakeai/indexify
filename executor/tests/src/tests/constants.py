tls_config = {
    "use_tls": True,
    "tls_config": {
        "ca_bundle_path": "/path/to/ca_bundle.pem",
        "cert_path": "/path/to/cert.pem",
        "key_path": "/path/to/key.pem",
    },
}

cert_path = tls_config["tls_config"]["cert_path"]
key_path = tls_config["tls_config"]["key_path"]
ca_bundle_path = tls_config["tls_config"]["ca_bundle_path"]
service_url = "localhost:8900"
config_path = "test/config/path"
code_path = "test/code_path"
