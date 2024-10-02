# Kubernetes

## Cluster Creation

You'll need a k8s cluster first. While there are a lot of different ways to get
a cluster, if you're doing this locally, we recommend using [k3d][k3d].

[k3d]: https://k3d.io/v5.6.3/#releases

Note: the local example includes a basic ingress -
[components/ingress](kustomize/components/ingress). The ingress exposes the API
server and is required to use Indexify. If you're doing a different setup,
you'll want to make an ingress definition that is specific to your environment.

### Local

One way to create a cluster is using [k3d][k3d]. This will run a lightweight
version of Kubernetes entirely within docker on your local system.

```bash
k3d cluster create -p "8900:80@loadbalancer" indexify
```

When using this setup, Indexify will be exposed via k3d's ingress which will be
[http://localhost:8900](http://localhost:8900). You'll want to configure
`IndexifyClient(service_url="http://localhost:8900")`.

## Installation

### Helm

To run locally, you can install the chart using some
[pre-configured values](helm/local.yaml) and then go through the getting started
guide. To install, run:

```bash
helm install local helm -f helm/local.yaml -n indexify --create-namespace
```

The chart is configured to run in a local environment. To run in a production
environment, you'll want to make sure to configure the following:

- High Availability - by default, the coordinator starts up with a single
  replica. To run in HA mode, set the replicas to an odd number via
  `coordinator.replicas`.

- Blob Store - We're using minio for local development via the [official
  chart][minio]. `local.yaml` configures it to run without persistence. To use
  S3, set `minio.enabled=false` and make sure IAM has added the correct
  credentials for accessing S3. To use other blob stores that support S3's API,
  look into setting `blobStore.endpoint` and `blobStore.credentialSecret`.

- Persistence - By default, the Indexify server is configured to use the
  local filesystem as the stateful set storage backend. To use a cloud-based
  storage solution, set the `persistence.storageClassName` section to use your cloud provider's storage solution
  i.e. `persistence.storageClassName: "ebs-csi-default-sc"`.
