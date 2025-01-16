## Overview

Executor registers at Indexify Server and continuously pulls tasks assigned to it from the Indexify Server
and executes them. While registering it shares its capabilities like available hardware with the Indexify
Server and periodically updates the Server about its current state. Executor spins up Function Executors
to run customer functions. Executor should never link with Tensorlake Python-SDK. It should not know anything
about programming languages and runtime environments used by Tensorlake Functions. Function Executor is
responsible for this.

This subpackage doesn't provide an executable entry point that runs an Executor. This is intentional
as Executor has many configurable sub-components. indexify cli subpackage provides `executor`
command that runs Executor with functionality available in Open Source offering.

## Deployment

### Production setup

A single Executor runs in a Virtual Machine, container or a in bare metal host. An Indexify cluster
is scaled by adding more Executor hosts. Open Source users manage and scale the hosts themselves e.g.
using Kubernetes, any other orchestrator or even manually. E.g. the users provision secrets,
persistent volumes to each host using the orchestrator or manually. Each Executor runs a single function.
The function name and other qualifiers are defined in Executor arguments.

### Development setup

To make Indexify development and testing easier an Executor in development mode can run any function.
Running multiple Executors on the same host is supported too. In this case each Executor requires a
unique port range passed to it in its arguments.

## Threat model

A VM/container/bare metal host where an Executor is running is fully trusted. This works well for single
tenant deployments where customer functions' code is fully trusted. If this is not the case then Function
Executors that run customer functions need to get isolated from Executor using e.g. Virtual Machines.
This functionality is not included into the Open Source offering.
