# Kubernetes

If you'd like to try with your own cluster, check out the
[instructions][operations/k8s]. They'll walk you through an ephemeral setup
using a local cluster. To get Indexify into production, you'll want to modify
the YAML so that it works with your environment. In particular, make sure to pay
attention to the dependencies.

[operations/k8s]:
  https://github.com/tensorlakeai/indexify/tree/main/operations/k8s

## Components

- [API Server][api.yaml] - This is where all your requests go. There's an
  ingress which exposes `/` by default.
- [Coordinator][coordinator.yaml] - Task scheduler than manages handing work out
  to the extractors.
- [Extractors][extractor.yaml] - Extractors can take multiple forms, this
  example is generic and works for all the extractors which are distributed by
  the project.

[api.yaml]:
  https://github.com/tensorlakeai/indexify/blob/main/operations/k8s/kustomize/base/api.yaml
[coordinator.yaml]:
  https://github.com/tensorlakeai/indexify/blob/main/operations/k8s/kustomize/base/coordinator.yaml
[extractor.yaml]:
  https://github.com/tensorlakeai/indexify/blob/main/operations/k8s/kustomize/components/extractor/extractor.yaml

## Dependencies

### Blob Store

We recommend using an S3 like service for the blob store. Our [ephemeral
example][kustomize/local] uses minio for this. See the [environment variable
patch][minio/api.yaml] for how this gets configured.

[kustomize/local]:
  https://github.com/tensorlakeai/indexify/blob/main/operations/k8s/kustomize/local/kustomization.yaml
[minio/api.yaml]:
  https://github.com/tensorlakeai/indexify/blob/main/operations/k8s/kustomize/components/minio/api.yaml

#### GCP

- You'll want to create a [HMAC key][gcp-hmac] to use as `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY`.
- Set `AWS_ENDPOINT_URL` to `https://storage.googleapis.com/`

[gcp-hmac]: https://cloud.google.com/storage/docs/authentication/hmackeys

#### Other Clouds

Not all clouds expose a S3 interface. For those that don't check out the
[s3proxy][s3proxy] project. However, we'd love help implementing your native
blob storage of choice! Please open an [issue][issue] so that we can have a
discussion on how that would look for the project.

[s3proxy]: https://github.com/gaul/s3proxy
[issue]: https://github.com/tensorlakeai/indexify/issues

### Vector Store

We support multiple backends for vectors including `LancDb`, `Qdrant` and
`PgVector`. The ephemeral example uses postgres and `PgVector` for this. The
[database][vector-store.yaml] itself is pretty simple. Pay extra attention to
the [patch][postgres/config.yaml] which configures the API server and collector
to use that backend.

[vector-store.yaml]:
  https://github.com/tensorlakeai/indexify/blob/main/operations/k8s/kustomize/components/postgres/vector-store.yaml
[postgres/config.yaml]:
  https://github.com/tensorlakeai/indexify/blob/main/operations/k8s/kustomize/components/postgres/config.yaml

### Structured Store

Take a look at the vector store component in kustomize. It implements the
structured store as well.
