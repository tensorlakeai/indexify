use opentelemetry_sdk::metrics::SdkMeterProvider;

pub mod as_metrics {
    use std::sync::{Arc, Mutex};

    use anyhow::anyhow;
    use opentelemetry::{global::meter, metrics::ObservableCounter};
    use state_store::IndexifyState;

    use crate::http_objects::IndexifyAPIError;

    #[derive(Clone)]
    pub struct MetricsData {
        unallocated_tasks: ObservableCounter<u64>,
    }

    impl MetricsData {
        pub fn new(indexify_state: Arc<IndexifyState>) -> MetricsData {
            let unallocated_tasks = meter("unallocated_tasks_counter")
                .u64_observable_counter("unallocated_tasks")
                .with_description("Counter that displays unallocated tasks in the server")
                .with_callback({
                    let indexify_state_lock = Mutex::new(indexify_state.clone());

                    move |observer| {
                        let indexify_state = indexify_state_lock.lock().unwrap();
                        let unallocated_tasks = indexify_state.reader().unallocated_tasks();

                        match unallocated_tasks {
                            Ok(tasks) => {
                                for task in tasks {
                                    let compute_graph = indexify_state
                                        .reader()
                                        .get_compute_graph(
                                            &task.namespace,
                                            &task.compute_graph_name,
                                        )
                                        .map_err(|_| {
                                            IndexifyAPIError::internal_error(anyhow!(
                                                "Unable to read metrics"
                                            ))
                                        })
                                        .unwrap();

                                    let compute_graph = match compute_graph {
                                        None => {
                                            continue;
                                        }
                                        Some(x) => x,
                                    };

                                    let node =
                                        compute_graph.nodes.get(&task.compute_fn_name).unwrap();

                                    let image_version = node.image_version();
                                    let image_name = node.image_name().to_string();

                                    let version_kv = opentelemetry::KeyValue::new(
                                        "image_version",
                                        image_version.to_string(),
                                    );
                                    let name_kv =
                                        opentelemetry::KeyValue::new("image_name", image_name);
                                    observer.observe(1, &[version_kv, name_kv])
                                }
                            }
                            Err(_) => {}
                        }
                    }
                })
                .init();

            MetricsData { unallocated_tasks }
        }
    }
}

pub fn init_provider() -> prometheus::Registry {
    let registry = prometheus::Registry::new();

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build();

    let mut provider = SdkMeterProvider::builder();

    if let Ok(exporter) = exporter {
        provider = provider.with_reader(exporter);
    };

    opentelemetry::global::set_meter_provider(provider.build());

    registry
}
