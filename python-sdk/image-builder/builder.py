
import structlog
import hashlib
import base64

import docker
import docker.api.build
import docker.api.image

import boto3

from indexify.http_client import IndexifyClient
from indexify.functions_sdk.image import ImageInformation

logger = structlog.get_logger(module=__name__)

class ImageBuildSpec:
    def __init__(self, namespace, sdk_version, name, base_image, run_strs):
        self.namespace = namespace
        self.sdk_version = sdk_version
        self.name = name
        self.base_image = base_image
        self.run_strs = run_strs

    def hashFromImageInfo(self):
        hash = hashlib.sha256(self.name.encode()) # Make a hash of the image name
        hash.update(self.base_image.encode())
        hash.update("".join(self.run_strs).encode())
        hash.update(self.sdk_version.encode())
        return hash.hexdigest()

    def taggedImageName(self):
        return f"{self.imageName()}:{self.hashFromImageInfo()}"
        
    def imageName(self):
        return f"{self.namespace}/{self.name}"
    
    @classmethod
    def fromImageInformation(cls, namespace, sdk_version, info: ImageInformation):
        return cls(namespace, sdk_version, info.image_name, info.base_image, info.run_strs)


class Builder:
    def __init__(self, registry_driver, service_url="http://localhost:8900"):
        self.indexify = IndexifyClient(service_url=service_url)
        self.docker = docker.from_env()
        self.docker.ping()
        self.registry = registry_driver

        self.images_in_process = set()  # Images in process, 

    def run(self):
        """Run local containers for all graphs.
        """
        foundImages = {}
        for namespace in self.indexify.namespaces():
            logger.debug(f"Scanning namespace {namespace}")

            for graph in self.indexify.graphs(namespace=namespace):
                logger.debug(f"Found graph {graph.name}")
                for node in graph.nodes.values():
                    if node.compute_fn is not None:
                        spec = ImageBuildSpec.fromImageInformation(namespace, graph.runtime_information.sdk_version, node.compute_fn.image_information)
                        foundImages[spec.name] = spec.taggedImageName()

                    if node.dynamic_router is not None:
                        spec = ImageBuildSpec.fromImageInformation(namespace, graph.runtime_information.sdk_version, node.dynamic_router.image_information)
                        foundImages[spec.name] = spec.taggedImageName()        
            
        containers = {}
        for container in self.docker.containers.list():
            containers[container.name] = container

        for image_name, image in foundImages.items():
            container_name = image_name.replace("/", "-")

            if container_name in containers: # If there is a running container, make sure it's using the right image
                container = containers[container_name]
                logger.debug(f"Found matching container {container_name} with image {container.image.id}")
                if image not in container.image.attrs['RepoTags']:
                    logger.info(f"Container image is tagged for stale version of {image_name}, reloading with fresh image")
                    container.stop()
                    container.remove()
                    self.docker.containers.run(image, ["indexify-cli", "executor", "--server-addr", "host.docker.internal:8900"], name=container_name, detach=True)
                    logger.debug(f"New container running with image {image}")

                else:
                    logger.debug(f"Running container {container_name} matches current version of the image, leaving alone")

            else:
                logger.info(f"Starting container for {image_name}")
                self.docker.containers.run(image, ["indexify-cli", "executor", "--server-addr", "host.docker.internal:8900"], name=container_name, detach=True)
        
    def scan(self):
        """Scan the state of the server and look for image definitions.
        """
        foundImages = [] # imageName:tag = image_info

        for namespace in self.indexify.namespaces():
            logger.debug(f"Scanning namespace {namespace}")

            for graph in self.indexify.graphs(namespace=namespace):
                logger.debug(f"Found graph {graph.name}")
                for node in graph.nodes.values():
                    if node.compute_fn is not None:
                        foundImages.append(ImageBuildSpec.fromImageInformation(namespace, graph.runtime_information.sdk_version, node.compute_fn.image_information))
                        
                    if node.dynamic_router is not None:
                        foundImages.append(ImageBuildSpec.fromImageInformation(namespace, graph.runtime_information.sdk_version, node.dynamic_router.image_information))
                    
        # Process found images, create repos if necessary and reject images that are already being built
        imagesToBeBuilt = []
        processedImages = []
        for spec in foundImages:
            repoName = spec.imageName()
            taggedName = spec.taggedImageName()
            
            if taggedName in processedImages:
                logger.debug(f"Skipping {taggedName} since we already checked it")
                continue
            processedImages.append(taggedName)

            if self.registry.repoExists(repoName):
                if self.registry.imageExists(taggedName):
                    logger.info(f"Image {taggedName} already exists in repo, skipping")
                    continue
            else:
                self.registry.repoCreate(repoName)
            
            # If we got here the image is being built
            imagesToBeBuilt.append(spec)

        return imagesToBeBuilt

    def buildImage(self, spec: ImageBuildSpec, docker_client=None, sdk_path=None):
        tag = spec.taggedImageName()

        docker_contents = [f"FROM {spec.base_image}", 
                           "RUN mkdir -p ~/.indexify", 
                           "RUN touch ~/.indexify/image_name", 
                           f"RUN  echo {spec.name} > ~/.indexify/image_name",
                           "WORKDIR /app"]

        docker_contents.extend(["RUN " + i for i in spec.run_strs])

        if sdk_path is not None:
            logger.info(f"Building image {tag} with local version of the SDK")
            docker_contents.append(f"COPY {sdk_path} /app/python-sdk")
            docker_contents.append("RUN (cd /app/python-sdk && pip install .)")
        else:
            docker_contents.append(f"RUN pip install indexify=={spec.sdk_version}")

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
        
        logger.info(f"Built image {tag}; arch:{image.attrs['Architecture']}; linux:{image.attrs["Os"]}; size:{image.attrs["Size"]}")
        return image
    
    def pushImage(self, spec: ImageBuildSpec, docker_client=None):
        if docker_client is None:
            docker_client = self.docker_client
        self.registry.imagePush(spec.taggedImageName(), docker_client)

class RegistryDriver:
    """Abstract class for registry implementations
    """

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
        logger.info(f"Pushing {image_name} to ECR")
        repo_name, image_tag = image_name.split(":")
        image = docker_client.images.get(image_name)

        # Get the ARN of the repository
        result = self.client.describe_repositories(repositoryNames=[repo_name])
        repositoryUri = result['repositories'][0]['repositoryUri']

        logger.debug(f"Repo URI is {repositoryUri}")
        image.tag(repositoryUri, tag=image_tag)

        # Authenticate with ECR
        authResp = self.client.get_authorization_token()
        token = base64.b64decode(authResp['authorizationData'][0]['authorizationToken']).decode()
        username, password = token.split(':')
        auth_config = {'username': username, 'password': password}

        logger.debug("Authenticated with ECR, starting to push now")
        start_time = time.time()
        response = docker_client.images.push(repositoryUri, tag=image_tag, auth_config=auth_config)
        logger.info(f"Push to ECR complete, took {time.time() - start_time} seconds")
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

    def buildTask(spec):
        docker_client = docker.from_env()
        docker_client.ping()

        # TODO: When running pool we need to mark this as in-process to avoid double scheduling
        image = b.buildImage(spec, sdk_path="./", docker_client=docker_client) 
        b.pushImage(spec, docker_client=docker_client)
        # TODO: Mark the image build as complete.... somewhere

        # TODO: Start SOCI indexing or something else to make loading faster

    while True:
        build_specs = b.scan() # This happens globally on a single thread
        
        # In a thread pool....
        for spec in build_specs:
            buildTask(spec) # TODO: Put this in the pool
        
        b.run()

        time.sleep(10)
