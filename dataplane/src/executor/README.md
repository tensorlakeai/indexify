# executor

The Indexify Executor is a component that is in charge of spawning/allocating tasks to Tensorlake's Function Executor. It coordinates between the Function Executor and the Indexify Server.

## Components

* Monitoring Server: Connects to Indexify server and reports its health. It serves a HTTP server on a configurable port, running on 7000 by default.
    * `/monitoring/startup`: Tells whether the Executor ready to receive tasks. Doesn't report the health of the connection to the server, thus doesn't indicate if tasks will successfully run.
    * `/monitoring/health`: Tells whether the Executor has an healthy connection to the Indexify server.
    * `/monitoring/metrics`: Exposes the Executor's prometheus metrics.
    * `/state/reported`: Reports the metadata state of the Executor (host). These include the available host resources, dev environment setup, OS, CPU architecture, state hash, system time, and executor id.
    * `/state/desired`: Reports the desired state of the Executor (host) received from the Indexify server.
* State Reconciler: Executes functions based on Executor initial state and updates the evenutal state.

## Design decisions

* Metrics: We consolidate the metrics into a single module in order to simplify state sharing across the Executor modules and threads.
* gRPC: The gRPC client API is also consolidated into a single module for simplifying implementation and for no other apparent reason.
