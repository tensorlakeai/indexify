

import docker.api.image
import structlog
import hashlib
import base64
import pprint
import sys

import docker
import docker.api.build
import boto3

from indexify.http_client import IndexifyClient
from indexify.functions_sdk.image import ImageInformation


logger = structlog.get_logger(module=__name__)

class Builder:
    def __init__(self, registry_driver, service_url="http://localhost:8900"):
        self.indexify = IndexifyClient(service_url=service_url)
        self.docker = docker.from_env()
        self.docker.ping()
        self.registry = registry_driver

        self.images_in_process = set()  # Images in process, 

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
        foundImages = {} # imageName:tag = image_info

        for namespace in self.indexify.namespaces():
            logger.debug(f"Scanning namespace {namespace}")

            for graph in self.indexify.graphs(namespace=namespace):
                logger.debug(f"Found graph {graph.name}")
                for node in graph.nodes.values():
                    if node.compute_fn is not None:
                        imageHash = self.hashFromImageInfo(node.compute_fn.image_information)
                        imageKey = f"{namespace}/{graph.name}:{imageHash}"
                        foundImages[imageKey] = node.compute_fn.image_information

                    if node.dynamic_router is not None:
                        imageHash = self.hashFromImageInfo(node.dynamic_router.image_information)
                        imageKey = f"{namespace}/{graph.name}:{imageHash}"
                        foundImages[imageKey] = node.dynamic_router.image_information

        # Process found images, create repos if necessary and reject images that are already being built
        imagesToBeBuilt = {}
        
        for image, image_info in foundImages.items():
            image_name, image_tag = image.split(":")

            if self.registry.repoExists(image_name):
                if self.registry.imageExists(image):
                    logger.info(f"Image {image} already exists in repo, skipping")
                    continue
            else:   
                self.registry.repoCreate(image_name)
            
            # If we got here the image is being built
            imagesToBeBuilt[image] = image_info

        return imagesToBeBuilt

    def buildImage(self, tag, image_info:ImageInformation, sdk_path=None, docker_client=None):
        docker_contents = [f"FROM {image_info.base_image}", 
                           "RUN mkdir -p ~/.indexify", 
                           "RUN touch ~/.indexify/image_name", 
                           f"RUN  echo {image_info.image_name} > ~/.indexify/image_name",
                           "WORKDIR /app"]

        docker_contents.extend(["RUN " + i for i in image_info.run_strs])

        if sdk_path:
            logger.info(f"Building image {tag} local version of the SDK")
            docker_contents.append(f"COPY {sdk_path} /app/python-sdk")
            docker_contents.append("RUN (cd /app/python-sdk && pip install .)")
        else:
            docker_contents.append(f"RUN pip install indexify=={image_info.sdk_version}") ## TODO: Pull the indexify version from the client somehow

        docker_file = "\n".join(docker_contents)

        docker.api.build.process_dockerfile = lambda dockerfile, path: (
            "Dockerfile",
            dockerfile,
        )

        if docker_client is None:
            docker_client = self.docker

        image, logs = self.docker.images.build(
            path="./",
            dockerfile=docker_file,
            tag=tag,
            rm=True
        )

        return image
    
    def pushImage(self, image_name, docker_client=None):
        if docker_client is None:
            docker_client = self.docker_client
            
        self.registry.login(docker_client)
        self.registry.imagePush(image_name, docker_client)

class RegistryDriver:
    """Abstract class for registry implementations
    """
    def login(self, docker_client:docker.DockerClient):
        raise NotImplemented
    
    def repoCreate(self, name):
        raise NotImplemented

    def repoExists(self, name):
        raise NotImplemented

    def imagePush(self, image_name, docker_client:docker.DockerClient):
        raise NotImplemented

    def imageExists(self, image):
        raise NotImplemented

class ECRDriver(RegistryDriver):
    """Implementation of ECR driver
    """
    def __init__(self, region="us-east-1"):
        self.region = region
        self.client = boto3.client('ecr', region_name=self.region)

    def login(self, docker_client:docker.DockerClient):
        token = self.client.get_authorization_token()
        username, password = base64.b64decode(token['authorizationData'][0]['authorizationToken']).decode().split(':')
        registry = token['authorizationData'][0]['proxyEndpoint']
        result = docker_client.login(username, password, registry=registry)
        
    def repoCreate(self, repo_name:str):
        self.client.create_repository(repositoryName=repo_name, 
                                      imageTagMutability="MUTABLE", 
                                      imageScanningConfiguration={
                                          "scanOnPush": True, 
                                      },
                                      tags=[
                                          {"Key": "Owner", 
                                           "Value":"image-builder"}
                                          ])
        
    def repoExists(self, repo_name):
        try:
            self.client.describe_repositories(repositoryNames=[repo_name])
        except self.client.exceptions.RepositoryNotFoundException:
            return False
        return True

    def imagePush(self, image_name, docker_client:docker.DockerClient):
        repo_name, image_tag = image_name.split(":")

        image = docker_client.images.get(image_name)

        # Get the ARN of the repository
        result = self.client.describe_repositories(repositoryNames=[repo_name])
        repositoryUri = result['repositories'][0]['repositoryUri']
        image.tag(repositoryUri, tag=image_tag)

        response = docker_client.images.push(repositoryUri, tag=image_tag)
        print(response)
        
    def imageExists(self, image):
        repositoryName, tag = image.split(":")
        try:
            response = self.client.describe_images(
                repositoryName=repositoryName,
                imageIds=[
                    {
                        'imageTag': tag
                    },
                ],
            )
        except self.client.exceptions.ImageNotFoundException:
            return False
        
        return True

if __name__ == "__main__":
    import time
    from multiprocessing import Pool        
    ecrDriver = ECRDriver(region="us-east-1")
    b = Builder(ecrDriver)
    executor = Pool(3)

    while True:
        images = b.scan() # This happens globally on a single thread
        
        # In a thread pool....
        for image_name, image_info in images.items():
            docker_client = docker.from_env()
            docker_client.ping()

            # TODO: Mark the image as building, set the tag that we are going to use
            image = b.buildImage(image_name, image_info, sdk_path="./", docker_client=docker_client)
            b.pushImage(image_name, docker_client=docker_client)
            
            # TODO: If there aren't any issues building the image, mark any 
            # TODO: Start SOCI indexing or something else to make loading faster
        
        time.sleep(10)
