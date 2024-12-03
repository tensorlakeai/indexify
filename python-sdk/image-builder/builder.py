
import docker
import docker.api.build

from indexify.http_client import IndexifyClient
from indexify.functions_sdk.image import ImageInformation
import logging
import hashlib

class Builder:
    def __init__(self, service_url="http://localhost:8900"):
        self._server_url = service_url
        self._client = IndexifyClient(service_url=service_url)
        self._images = {}

        self.docker_client = docker.from_env()
        self.docker_client.ping()

    def hashFromImageInfo(self, image_info:ImageInformation):
        hash = hashlib.sha256(image_info.image_name.encode()) # Make a hash of the image name
        hash.update(image_info.image_name.encode())
        hash.update(image_info.tag.encode())
        hash.update(image_info.base_image.encode())
        hash.update("".join(image_info.run_strs).encode())
        hash.update(image_info.sdk_version.encode())
        
        return hash.hexdigest()

    def scan(self):
        """Scan the state of the server and look for image definitions.
        """
        foundImages = {} # namespace:graph:version:image_name

        for namespace in self._client.namespaces():            
            for graph in self._client.graphs(namespace=namespace):
                for node in graph.nodes.values():
                    if node.compute_fn is not None:
                        imageHash = self.hashFromImageInfo(node.compute_fn.image_information)
                        imageKey = f"ecr-repo:{namespace}-{graph.name}-{imageHash}-{graph.version}"
                        foundImages[imageKey] = node.compute_fn.image_information

                    if node.dynamic_router is not None:
                        imageHash = self.hashFromImageInfo(node.dynamic_router.image_information)
                        imageKey = f"ecr-repo:{namespace}-{graph.name}-{imageHash}-{graph.version}"
                        foundImages[imageKey] = node.dynamic_router.image_information

        return foundImages


    def buildImage(self, image_info:ImageInformation, sdk_path=None, tag=None):
        docker_contents = [f"FROM {image_info.base_image}", 
                           "RUN mkdir -p ~/.indexify", 
                           "RUN touch ~/.indexify/image_name", 
                           "WORKDIR /app"]

        docker_contents.extend(["RUN " + i for i in image_info.run_strs])

        if sdk_path:
            logging.info("Using local version of the SDK")
            docker_contents.append(f"COPY {sdk_path} /app/python-sdk")
            docker_contents.append("RUN (cd /app/python-sdk && pip install .)")
        else:
            docker_contents.append(f"RUN pip install indexify=={image_info.sdk_version}") ## TODO: Pull the indexify version from the client somehow

        docker_file = "\n".join(docker_contents)

        if tag is None:
            tag = image_info.tag

        docker.api.build.process_dockerfile = lambda dockerfile, path: (
            "Dockerfile",
            dockerfile,
        )

        image, logs = self.docker_client.images.build(
            path="./",
            dockerfile=docker_file,
            tag=tag,
            rm=True
        )

        for entry in logs:
            print(entry)

if __name__ == "__main__":
    b = Builder()
    images = b.scan() # This happens globally on a single thread

    # In a thread pool....
    for image_key, image_info in images.items():
        # TODO: Mark the image as building, set the tag that we are going to use
        b.buildImage(image_info, sdk_path=None, tag=image_key)
        
        # TODO: If there aren't any issues building the image, mark any 
        # TODO: Start SOCI indexing or something else to make loading faster
