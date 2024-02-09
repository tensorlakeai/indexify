# tests/indexify_extractor_sdk/test_packager.py

from indexify_extractor_sdk.packager import ExtractorPackager, ExtractorPackagerConfig
from unittest import mock
import pytest

@pytest.fixture
def extractor_config():
    return ExtractorPackagerConfig(
        module_name="indexify_extractor_sdk.mock_extractor",
        class_name="MockExtractor",
        dockerfile_template_path="dockerfiles/Dockerfile.extractor",
        verbose=True,
        dev=False,
        gpu=False
    )

@pytest.fixture
def extractor_config_dev():
	return ExtractorPackagerConfig(
		module_name="indexify_extractor_sdk.mock_extractor",
		class_name="MockExtractor",
		dockerfile_template_path="dockerfiles/Dockerfile.extractor",
		verbose=True,
		dev=True,
		gpu=False
	)

@pytest.fixture
def packager(extractor_config):
    return ExtractorPackager(extractor_config)

@pytest.fixture
def packager_dev(extractor_config_dev):
	return ExtractorPackager(extractor_config_dev)

@pytest.fixture
def jinja_template():
    # get the jinja template from the Dockerfile.extractor file
    with open("dockerfiles/Dockerfile.extractor", "r") as f:
        return f.read()

def test_generate_dockerfile(packager, jinja_template):
    # Mock the file read operation to return a known template string
    with mock.patch("builtins.open", mock.mock_open(read_data=jinja_template)):
        dockerfile_content = packager._generate_dockerfile()
        # print the dockerfile_content to see the actual content
        assert "apt-get install -y  sl cowsay" in dockerfile_content
        assert "RUN pip3 install --no-input --extra-index-url https://download.pytorch.org/whl/cpu tinytext pyfiglet" in dockerfile_content
        assert "RUN pip3 install --no-input indexify_extractor_sdk --extra-index-url https://download.pytorch.org/whl/cpu" in dockerfile_content
        
def test_generate_dockerfile_dev(packager_dev, jinja_template):
    # Mock the file read operation to return a known template string
    with mock.patch("builtins.open", mock.mock_open(read_data=jinja_template)):
        dockerfile_content = packager_dev._generate_dockerfile()
        # print the dockerfile_content to see the actual content
        assert "apt-get install -y  sl cowsay" in dockerfile_content
        assert "RUN pip3 install --no-input --extra-index-url https://download.pytorch.org/whl/cpu tinytext pyfiglet" in dockerfile_content
        assert "COPY indexify_extractor_sdk /indexify/indexify_extractor_sdk" in dockerfile_content
        assert "COPY setup.py /indexify/setup.py" in dockerfile_content
        assert "RUN python3 setup.py install" in dockerfile_content
        
def test_generate_compressed_tarball(packager):
    dockerfile_content = "FROM python:3.8"
    tarball_bytes = packager._generate_compressed_tarball(dockerfile_content)
    assert tarball_bytes is not None
    assert isinstance(tarball_bytes, bytes)
    # To further test the contents of the tarball, you'd need to decompress and untar it here,
    # then check if the Dockerfile exists within the tarball with the expected content.

@pytest.mark.skip(reason="This test will build the docker image and will take time")
def test_package(packager):
    packager.package()