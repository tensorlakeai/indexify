# executor

The Indexify Executor is a component that is in charge of spawning/allocating tasks to Tensorlake's Function Executor. It coordinates between the Function Executor and the Indexify Server.

## Components

* Monitoring Server: Connects to Indexify server and reports its health. It serves a HTTP server on a configurable port, running on 7000 by default.
    * `/monitoring/startup`: Tells whether the Executor ready to receive tasks. Doesn't report the health of the connection to the server, thus doesn't indicate if tasks will successfully run.
    * `/monitoring/health`: Tells whether the Executor has an healthy connection to the Indexify server.
    * `/monitoring/metrics`: Exposes the Executor's prometheus metrics.
    * `/state/reported`: Reports the metadata state of the Executor (host). These include the available host resources, dev environment setup, OS, CPU architecture, state hash, system time, and executor id.
    * `/state/desired`: Reports the desired state of the Executor (host) received from the Indexify server.
* State Reconciler: Runs Function Executor (Controller) based on Executor initial state and updates the eventual state based on the provided data.
* Host Resources: Gets the local available resources of the host. These include: CPU information, available memory, disk space, and GPU information (via nvidia-smi).
* Blob Store: Gives limited permissions to Tensorlake Function Executor Workers. Has access to local file storage as well as AWS S3 but doesn't by itself manage the buckets. It's specific use in the context of the Executor is to presign URIs for Workers. Presigning with local file storage is redundant.
* Function Executor (Controller): Spins up and manages the Tensorlake Function Executor Worker binary. Feeds the Worker with data it needs such as presigned URIs, cached application code, etc. It is in charge of making sure the Worker is running and healthy, and the function lifecycle is as stable and secure as possible.

## Design decisions

* Metrics: We consolidate the metrics into a single module in order to simplify state sharing across the Executor modules and threads.
* gRPC: The gRPC client API is also consolidated into a single module for simplifying implementation and for no other apparent reason.

## Dependencies

* Tensorlake python package
* Indexify server
* Nvidia-smi
* AWS S3 config
