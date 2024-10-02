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
  storage solution, set the `persistence.storageClassName` section to use your cloud provider's storage solution.

### Kustomize

Create a namespace for Indexify:

```bash
kubectl create namespace indexify
```

To run locally, you can apply the [local](kustomize/local) setup and then go
through the getting started guide. To install, run:

```bash
kubectl apply -k kustomize/local
```

There are optional components that you can use as part of your Indexify
installation. To make this possible, the optional pieces have been split out
into separate components. The minio example is not meant to be
run in production. Make sure to create your own in a way that reflects your
environment.

- [base](kustomize/base) - this includes the API server and the executor deployment.
- [components/ingress](kustomize/components/ingress) - a basic ingress
  definition used to get access to the API server as part of the local install.
- [components/minio](kustomize/components/minio) - an ephemeral example of using
  S3 for blog storage.

> [!NOTE] The API server comes with an ingress resource by default that exposes
> the api at `/`. Make sure to change this if you'd like it at a different
> location.

#### Customization

To customize the installation so that it works in your environment, take a look
at [local/kustomize.yaml](kustomize/local/kustomization.yaml). Each resource and
component entry are optional and can be swapped out to use your own solution.

For example, if you would like to use S3 instead of minio but otherwise leave
the example intact, you would write a `kustomization.yaml` file that looks like:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: indexify

resources:
  - ../base
  - ../components/ingress

patches:
  # base/api.yaml
  - patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: indexify-server
        labels:
          app.kubernetes.io/component: indexify-server
      spec:
        template:
          spec:
            containers:
              - name: indexify-server
                env:
                  # Ideally, this config is coming from IAM in your cluster.
                  - name: AWS_ACCESS_KEY_ID
                    value: XXXX
                  - name: AWS_SECRET_ACCESS_KEY
                    value: XXXX
                volumeMounts:
                  - name: data
                    mountPath: /tmp/indexify-blob-storage
        volumeClaimTemplates:
          - metadata:
              name: data
            spec:
              storageClassName: gp2
              accessModes:
                - ReadWriteOnce
              resources:
                requests:
                  storage: 1Gi

labels:
  - includeSelectors: true
    pairs:
      app.kubernetes.io/part-of: indexify
```

This will apply the base and ingress components and patch the API server to use
S3 instead of minio. You'll need to make sure that the `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` are set to the correct values for your environment.

The volume claim template is set to use `gp2` as the storage class. Make sure to
change this to the correct storage class for your environment.

[minio]: https://github.com/minio/minio/tree/master/helm/minio
