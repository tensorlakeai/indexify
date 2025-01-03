## Overview

Executor registers at Indexify Server and continiously pulls tasks assigned to it from the Indexify Server
and executes them. While registering it shares its capabilities like available hardware with the Indexify
Server and periodically updates the Server about its current state. Executor spins up Function Executors
to run customer functions. Executor should never link with Indexify Python-SDK. It should not know anything
about programming languages and runtime environments used by Indexify Functions. Function Executor is
responsible for this.

## Deployment

An Executor can run in a Virtual Machine, container or a in bare metal host. Each vm/container/bare metal
host in an Indexify cluster deployment runs a single Executor daemon process.
Open Source users manage the Executors fleet themself e.g. using Kubernetes, any other cluster
orchestrator or even manually.

An Executor can be configured to only run particular functions. This allows to route certain Indexify
functions to only particular subsets of Executors to e.g. implement a load balancing strategy.

This package doesn't provide an executable entry point that runs an Executor. This is intentional
as Executor has many configurable subcomponents. `indexify` package provides a cli with `executor`
command that runs Executor with functionality available in Open Source offering.

## Threat model

A VM/container/bare metal host where an Executor is running is fully trusted. This works well for single
tenant deployments where customer functions' code is fully trusted. If this is not the case then Function
Executors that run customer functions need to get isolated from Executor using e.g. Virtual Machines.
This functionality is not included into the Open Source offering.
