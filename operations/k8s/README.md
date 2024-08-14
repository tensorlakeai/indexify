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

### Kustomize

To run locally, you can apply the [local](kustomize/local) setup and then go
through the getting started guide. To install, run:

```bash
kubectl apply -k kustomize/local
```

There are optional components that you can use as part of your Indexify
installation. To make this possible, the optional pieces have been split out
into separate components. The postgres and minio examples are not meant to be
run in production. Make sure to create your own in a way that reflects your
environment.

- [base](kustomize/base) - this includes the API server and the coordinator.
- [components/ingress](kustomize/components/ingress) - a basic ingress
  definition used to get access to the API server as part of the local install.
- [components/postgres](kustomize/components/postgres) - a simple, ephemeral
  example of using postgres for all database operations including the vector
  store.
- [components/minio](kustomize/components/minio) - an ephemeral example of using
  S3 for blog storage.
- [components/extractors](kustomize/components/extractor) - extractors are
  published as common containers, this component is used by all the extractors,
  such as [minilm-l6](kustomize/components/minilm-l6) to provide extraction.

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
  - ../components/chunker
  - ../components/minilm-l6

components:
  - ../components/postgres

patches:
  # base/api.yaml
  - patch: |-
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: api
        labels:
          app.kubernetes.io/component: api
      spec:
        template:
          spec:
            containers:
              - name: indexify
                env:
                  # Ideally, this config is coming from IAM in your cluster.
                  - name: AWS_ACCESS_KEY_ID
                    value: XXXX
                  - name: AWS_SECRET_ACCESS_KEY
                    value: XXXX
  # components/extractor/extractor.yaml
  - target:
      kind: Deployment
      labelSelector: app.kubernetes.io/component=extractor
    patch: |-
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: extractor
      spec:
        template:
          spec:
            containers:
              - name: extractor
                env:
                  # Ideally, this config is coming from IAM in your cluster.
                  - name: AWS_ACCESS_KEY_ID
                    valueFrom:
                      secretKeyRef:
                        name: blob-store
                        key: AWS_ACCESS_KEY_ID
                  - name: AWS_SECRET_ACCESS_KEY
                    valueFrom:
                      secretKeyRef:
                        name: blob-store
                        key: AWS_SECRET_ACCESS_KEY
  # base/config.yaml
  - patch: |-
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: indexify
      data:
        s3.yml: |-
          blob_storage:
            backend: s3
            s3:
              bucket: XXX-my-bucket
              region: us-east-1

labels:
  - includeSelectors: true
    pairs:
      app.kubernetes.io/part-of: indexify
```

These three patches will configure the installation for your environment.

- `base/api.yaml` - Modifies the `indexify` container in `deploy/api` to include
  the environment variables required for S3.
- `components/extractor/extractor.yaml` - Uses the labelSelector
  `app.kubernetes.io/component=extractor` to modify all extractors and add S3's
  env.
- `base/config.yaml` - Adds a key to the configmap used by the API server and
  coordinator. The content of these keys is concatenated into a single
  `config.yaml` file on startup as part of an `initContainer`.

#### Extractors

For each extractor you'd like to add, you'll want to create a new
`kustomization.yaml`. These will be included in your parent installation the
same way that the local example includes the chunker and minilm-l6 extractors.

For example , To add the PDF extractor, you'll want to create `pdfextractor/kustomization.yaml` under components.

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

components:
  - ../extractor

images:
  - name: tensorlake/extractor:latest
    newName: tensorlake/pdfextractor
    newTag: latest

patches:
  - target:
      group: apps
      version: v1
      kind: Deployment
      name: extractor
    patch: |-
      - op: replace
        path: /metadata/name
        value: pdfextractor
      - op: add
        path: /spec/selector/matchLabels/app.kubernetes.io~1name
        value: pdfextractor
      - op: add
        path: /metadata/labels/app.kubernetes.io~1name
        value: pdfextractor
      - op: add
        path: /spec/template/metadata/labels/app.kubernetes.io~1name
        value: pdfextractor
      - op: add
        path: /spec/template/spec/hostname
        value: pdfextractor
  - target:
      version: v1
      kind: Service
      name: extractor
    patch: |-
      - op: replace
        path: /metadata/name
        value: pdfextractor
      - op: add
        path: /metadata/labels/app.kubernetes.io~1name
        value: pdfextractor
      - op: add
        path: /spec/selector/app.kubernetes.io~1name
        value: pdfextractor
```

This new extractor can then be included in your own install
`staging/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: indexify

resources:
  - ../base
  - ../components/ingress
  - ../components/chunker
  - ../components/minilm-l6
  - ../components/pdfextractor
```

### Helm

To run locally, you can install the chart using some
[pre-configured values](helm/local.yaml) and then go through the getting started
guide. To install, run:

```bash
helm install local helm -f helm/local.yaml -n indexify --create-namespace
```

Like the kustomize installation, there are some optional pieces that are managed
via `values.yaml`.

- Blob Store - We're using minio for local development via the [official
  chart][minio]. `local.yaml` configures it to run without persistence. To use
  S3, set `minio.enabled=false` and make sure IAM has added the correct
  credentials for accessing S3. To use other blob stores that support S3's API,
  look into setting `blobStore.endpoint` and `blobStore.credentialSecret`.

- Database - We're using postgresql for local development via the [bitnami
  chart][postgresql]. `local.yaml` configures it to run as a single primary.
  Note that the bitnami postgres image does not come with PgVector, so we are
  [repackaging it](helm/docker/pgvector.dockerfile) with the correct files. To
  use your own database, look at setting `dbURL`, `indexConfig` and
  `metadataStorage` to the correct values.

- Extractors - You can add all the extractors you'd like via `extractors`. The
  `local.yaml` example includes a couple, add the image that contains the
  extractor you'd like and it will be installed as part of the chart.

[minio]: https://github.com/minio/minio/tree/master/helm/minio
[postgresql]: https://github.com/bitnami/charts/tree/main/bitnami/postgresql
