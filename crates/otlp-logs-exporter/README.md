# Otlp Logs Exporter

A very specialized Open Telemetry Logs exporter for usage at Tensorlake.

The main use case for this exporter is to support retries and exponential backoff sending events to an OpenTelemetry collector. The main SDK does not have a release with this support yet.

This crate is 100% custom for Tensorlake's use case. It doesn't implement many features that the general Otel Log exporter implements because it's designed just for what we need from it.
