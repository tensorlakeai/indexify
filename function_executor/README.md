## Overview

Function Executor is a process with an API that allows to load and run a customer Function in Indexify.
Each function run is a task. The tasks can be executed concurrently. The API client controls
the desired concurrency. Killing the process allows to free all the resources that a loaded customer
functon is using. This is helpful because the SDK doesn't provide any callbacks to customer code to free
resources it's using. Even if there was such callback customer code still might misbehave.

## Deployment

A Function Executor is created and destroyed by another component called Executor. It also calls the
Function Executor APIs. The server is not expected to be deployed or managed manually by Indexify users
as it's a low level component.

## Threat model

Customer code is assumed to be not trusted. Function Executor must not obtain any credentials that grant
access to resources not owned by the customer who owns the function.