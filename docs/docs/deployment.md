# Deployment 

You can download the Indexify binary and deploy them on your own infrastructure using your automation tool of choice. Here, we show how to run the Indexify control plane with the binary, and on Kubernetes on AWS, GCP and Azure. We have open sourced the Kubernetes deployment scripts, you could use that as a starting point.

## Production
The strategies of deploying to production will depend on throughput of API queries, number of documents stored and number of extractors extracting features. Kubernetes is the easiest way to deploy the service across clouds with all it's dependencies. We have included K8s deployment specifications, we expect users deploying the service in their environment to tweak the settings based on their scale and availability requirements. We will go through the steps to deploy our provided K8s deployment configuration on AWS EKS.

1. Create a new EKS Cluster
    If you are using AWS and need a test cluster environment, we have created a cluster configuration that can be easily bootstrapped with `eksctl`, it has the CSI driver for provisioning persistent volumes for storage services on EBS.
    ```shell
    eksctl create cluster -f deployment/k8s/cluster_config.yaml
    ```

2. Deploy the secrets and configuration
    Create the secrets and configuration objects which are going to be used by Indexify and other dependencies for retrieving credentials and service configurations.
    ```shell
    kubectl apply -f deployment/k8s/secrets.yaml
    kubectl apply -f deployment/k8s/indexify-configmap.yaml
    ```

3. Create the Persistent Volumes
    Create the PVs.
    ```shell
    kubectl apply -f deployment/k8s/persistence-volumes.yaml
    ```

4. Deploy Postgres and Qdrant
    Next deploy Qdrant and Postgres and wait for them to be ready before moving on to the next step.
    ```shell
    kubectl apply -f deployment/k8s/postgres-deployment.yaml
    kubectl apply -f deployment/k8s/qdrant-deployment.yaml
    ```

5. Deploy Indexify
   Finally deploy Indexify.
   ```shell
    kubectl apply -f deployment/k8s/indexify-deployment.yaml
   ```