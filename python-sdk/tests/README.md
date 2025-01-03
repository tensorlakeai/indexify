## Overview

Put here unit tests and integration tests for Python SDK.
This package is particularly a good fit for end to end integration tests that call
Python SDK -> Indexify Server -> Executor -> Function Executor.
Start Indexify Server and an Executor before running these tests.
Only tests that don't depend on configuration of the components are allowed here.
Put end to end tests that depend on component configurations into packages
that configure these components.