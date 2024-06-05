# Kubernetes

## Components

The resources have been split into separate components:

- [base](kustomize/base) - this includes the API server and the coordinator.
- [components/postgres](kustomize/components/postgres) - a simple, ephemeral
  example of using postgres for all database operations including the vector
  store.
- [components/minio](kustomize/components/minio) - an ephemeral example of using
  S3 for blog storage.
- [components/extractors](kustomize/components/extractors) - extractors are
  published as common containers, this component is used by all the extractors,
  such as [minilm-l6](kustomize/components/minilm-l6) to provide extraction.

> [!NOTE] The API server comes with an ingress resource by default that exposes
> the api at `/`. Make sure to change this if you'd like it at a different
> location.

To run locally, you can apply the [local](kustomize/local) setup and then go
through the getting started guide.

```bash
kubectl apply -k kustomize/local
```

## Cluster Standup

### Local

One way to create a cluster is using k3d. This will run a lightweight version of
Kubernetes ([k3s][k3s]) entirely within docker on your local system.

[k3s]: https://k3s.io

```bash
k3d cluster create -p "8081:80@loadbalancer" indexify
```

When using this setup, Indexify will be exposed via k3d's ingress which will be
[http://localhost:8081](http://localhost:8081). You'll want to configure
`IndexifyClient(service_url="http://localhost:8081")`.
