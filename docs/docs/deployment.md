# Deployment 

Indexify can be easily deployed using containers locally on a laptop for testing and evaluation or on a cluster for production usage.

## Local Deployment
The easiest way to deploy locally is through Docker Compose. We package a compose recipe in the repo which sets everything up and exposes the API on your local machine.

1. Close the repo.
    ```
    git clone https://github.com/diptanu/indexify.git
    ```
2. Start Docker Compose
    ```
    docker compose up
    ```
    If everything goes well, this should start all the dependencies and the server and bring up the API on `https://localhost:8900`

## Production
The strategies of deploying to production will depend on throughput of API queries, number of documents stored and number of extractors extracting features. Kubernetes is the easiest way to deploy the service across clouds with all it's dependencies. We have included K8s deployment specifications, we expect users deploying the service in their environment to tweak the settings based on their scale and availability requirements. We will go through the steps to deploy our provided K8s deployment configuration on AWS EKS.

1. Create a new EKS Cluster
    If you are using AWS and need a test cluster environment, we have created a cluster configuration that can be easily bootstrapped with `eksctl`, it has the CSI driver for provisioning persistent volumes for storage services on EBS.
    ```
    eksctl create cluster -f deployment/k8s/cluster_config.yaml
    ```

2. Deploy the secrets and configuration
    Create the secrets and configuration objects which are going to be used by Indexify and other dependencies for retrieving credentials and service configurations.
    ```
    kubectl apply -f deployment/k8s/secrets.yaml
    kubectl apply -f deployment/k8s/indexify-configmap.yaml
    ```

3. Create the Persistent Volumes
    Create the PVs.
    ```
    kubectl apply -f deployment/k8s/persistence-volumes.yaml
    ```

4. Deploy Postgres and Qdrant
    Next deploy Qdrant and Postgres and wait for them to be ready before moving on to the next step.
    ```
    kubectl apply -f deployment/k8s/postgres-deployment.yaml
    kubectl apply -f deployment/k8s/qdrant-deployment.yaml
    ```

5. Deploy Indexify
   Finally deploy Indexify.
   ```
    kubectl apply -f deployment/k8s/indexify-deployment.yaml
   ```