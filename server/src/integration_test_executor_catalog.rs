#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Result;

    use crate::{
        config::ExecutorCatalogEntry,
        data_model::{
            filter::{Expression, LabelsFilter, Operator},
            test_objects::tests::{self as test_objects, TEST_NAMESPACE},
            ApplicationState,
        },
        state_store::requests::{
            CreateOrUpdateComputeGraphRequest, RequestPayload, StateMachineUpdateRequest,
        },
        testing::TestService,
    };

    #[tokio::test]
    async fn test_invocation_with_unsatisfiable_and_satisfiable_labels() -> Result<()> {
        // Step 1: Create an executor catalog entry with label foo=bar
        let mut labels = HashMap::new();
        labels.insert("foo".to_string(), "bar".to_string());

        let catalog_entry = ExecutorCatalogEntry {
            name: "custom".to_string(), // name is unused when labels are populated
            cpu_cores: 1,
            memory_gb: 1,
            disk_gb: 1,
            gpu_models: vec![],
            labels,
        };

        // Initialize the test service with the catalog
        let test_srv = TestService::new_with_executor_catalog(vec![catalog_entry]).await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Step 2: Build a compute graph whose functions require foo==baz
        // (unsatisfiable)
        let mut compute_graph = test_objects::mock_application();
        compute_graph.name = "graph_unsatisfiable".to_string();
        compute_graph.state = ApplicationState::Active;

        let unsat_constraint = LabelsFilter(vec![Expression {
            key: "foo".to_string(),
            value: "baz".to_string(),
            operator: Operator::Eq,
        }]);

        // Apply the unsatisfiable placement constraints to all functions
        for compute_fn in compute_graph.nodes.values_mut() {
            compute_fn.placement_constraints = unsat_constraint.clone();
        }
        compute_graph.start_fn.placement_constraints = unsat_constraint.clone();

        // Step 3: Persist the compute graph
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: TEST_NAMESPACE.to_string(),
            application: compute_graph.clone(),
            upgrade_requests_to_current_version: true,
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(Box::new(cg_request)),
            })
            .await?;

        // Step 4: Run constraint validation which should disable the graph
        test_srv
            .service
            .graph_processor
            .validate_graph_constraints()
            .await?;
        test_srv.process_all_state_changes().await?;

        // Step 5: Verify that the compute graph has been disabled
        let in_memory = indexify_state.in_memory_state.read().await;
        let key = crate::data_model::Application::key_from(TEST_NAMESPACE, "graph_unsatisfiable");
        let stored_graph = in_memory
            .applications
            .get(&key)
            .expect("compute graph not found in state");

        println!("stored_graph: {stored_graph:?}");

        match &stored_graph.state {
            ApplicationState::Disabled { .. } => {}
            _ => panic!("Compute graph should be disabled due to unsatisfiable constraints"),
        }

        // Release the read guard before further updates
        drop(in_memory);

        // Step 6: Build a compute graph whose functions require foo==bar (satisfiable)
        let mut sat_compute_graph = test_objects::mock_application();
        sat_compute_graph.name = "graph_satisfiable".to_string();
        sat_compute_graph.state = ApplicationState::Active;

        let sat_constraint = LabelsFilter(vec![Expression {
            key: "foo".to_string(),
            value: "bar".to_string(),
            operator: Operator::Eq,
        }]);

        // Apply the satisfiable placement constraints to all functions
        for compute_fn in sat_compute_graph.nodes.values_mut() {
            compute_fn.placement_constraints = sat_constraint.clone();
        }
        sat_compute_graph.start_fn.placement_constraints = sat_constraint.clone();

        // Step 7: Persist the satisfiable compute graph
        let sat_cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: TEST_NAMESPACE.to_string(),
            application: sat_compute_graph.clone(),
            upgrade_requests_to_current_version: true,
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(Box::new(sat_cg_request)),
            })
            .await?;

        // Step 8: Run constraint validation again which should keep the graph active
        test_srv
            .service
            .graph_processor
            .validate_graph_constraints()
            .await?;
        test_srv.process_all_state_changes().await?;

        // Step 9: Verify that the compute graph remains active
        let in_memory = indexify_state.in_memory_state.read().await;
        let sat_key = crate::data_model::Application::key_from(TEST_NAMESPACE, "graph_satisfiable");
        let sat_stored_graph = in_memory
            .applications
            .get(&sat_key)
            .expect("satisfiable compute graph not found in state");

        match &sat_stored_graph.state {
            ApplicationState::Active => {}
            _ => panic!("Compute graph should remain active due to satisfiable constraints"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_validate_graph_constraints_multiple_graphs() -> Result<()> {
        // Step 1: Create an executor catalog entry with label foo=bar
        let mut labels = HashMap::new();
        labels.insert("foo".to_string(), "bar".to_string());

        let catalog_entry = ExecutorCatalogEntry {
            name: "custom".to_string(),
            cpu_cores: 1,
            memory_gb: 1,
            disk_gb: 1,
            gpu_models: vec![],
            labels,
        };

        // Initialize the test service with the catalog
        let test_srv = TestService::new_with_executor_catalog(vec![catalog_entry]).await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Build graph with no placement constraints (should remain active)
        let mut graph_valid_no = test_objects::mock_application();
        graph_valid_no.name = "graph_valid_no_constraints".to_string();
        graph_valid_no.state = ApplicationState::Active;

        // Build graph with satisfiable constraints (foo==bar)
        let mut graph_valid_constraints = test_objects::mock_application();
        graph_valid_constraints.name = "graph_valid_constraints".to_string();
        graph_valid_constraints.state = ApplicationState::Active;
        let sat_constraint = LabelsFilter(vec![Expression {
            key: "foo".to_string(),
            value: "bar".to_string(),
            operator: Operator::Eq,
        }]);
        for compute_fn in graph_valid_constraints.nodes.values_mut() {
            compute_fn.placement_constraints = sat_constraint.clone();
        }
        graph_valid_constraints.start_fn.placement_constraints = sat_constraint.clone();

        // Build graph with unsatisfiable constraints (foo==baz)
        let mut graph_invalid_baz = test_objects::mock_application();
        graph_invalid_baz.name = "graph_invalid_baz".to_string();
        graph_invalid_baz.state = ApplicationState::Active;
        let bad_constraint_baz = LabelsFilter(vec![Expression {
            key: "foo".to_string(),
            value: "baz".to_string(),
            operator: Operator::Eq,
        }]);
        for compute_fn in graph_invalid_baz.nodes.values_mut() {
            compute_fn.placement_constraints = bad_constraint_baz.clone();
        }
        graph_invalid_baz.start_fn.placement_constraints = bad_constraint_baz.clone();

        // Build graph with another unsatisfiable constraint (foo==qux)
        let mut graph_invalid_qux = test_objects::mock_application();
        graph_invalid_qux.name = "graph_invalid_qux".to_string();
        graph_invalid_qux.state = ApplicationState::Active;
        let bad_constraint_qux = LabelsFilter(vec![Expression {
            key: "foo".to_string(),
            value: "qux".to_string(),
            operator: Operator::Eq,
        }]);
        for compute_fn in graph_invalid_qux.nodes.values_mut() {
            compute_fn.placement_constraints = bad_constraint_qux.clone();
        }
        graph_invalid_qux.start_fn.placement_constraints = bad_constraint_qux.clone();

        // Persist all graphs
        for application in [
            graph_valid_no.clone(),
            graph_valid_constraints.clone(),
            graph_invalid_baz.clone(),
            graph_invalid_qux.clone(),
        ] {
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateOrUpdateComputeGraph(Box::new(
                        CreateOrUpdateComputeGraphRequest {
                            namespace: TEST_NAMESPACE.to_string(),
                            application,
                            upgrade_requests_to_current_version: true,
                        },
                    )),
                })
                .await?;
        }

        // Run constraint validation which should disable the unsatisfiable graphs
        test_srv
            .service
            .graph_processor
            .validate_graph_constraints()
            .await?;
        test_srv.process_all_state_changes().await?;

        // Verify states of graphs after validation
        let in_memory = indexify_state.in_memory_state.read().await;

        // Active graphs
        let key_valid_no =
            crate::data_model::Application::key_from(TEST_NAMESPACE, "graph_valid_no_constraints");
        match &in_memory
            .applications
            .get(&key_valid_no)
            .expect("graph not found")
            .state
        {
            ApplicationState::Active => {}
            _ => panic!("graph_valid_no_constraints should be active"),
        }

        let key_valid_constraints =
            crate::data_model::Application::key_from(TEST_NAMESPACE, "graph_valid_constraints");
        match &in_memory
            .applications
            .get(&key_valid_constraints)
            .expect("graph not found")
            .state
        {
            ApplicationState::Active => {}
            _ => panic!("graph_valid_constraints should be active"),
        }

        // Disabled graphs
        let key_invalid_baz =
            crate::data_model::Application::key_from(TEST_NAMESPACE, "graph_invalid_baz");
        match &in_memory
            .applications
            .get(&key_invalid_baz)
            .expect("graph not found")
            .state
        {
            ApplicationState::Disabled { .. } => {}
            _ => panic!("graph_invalid_baz should be disabled"),
        }

        let key_invalid_qux =
            crate::data_model::Application::key_from(TEST_NAMESPACE, "graph_invalid_qux");
        match &in_memory
            .applications
            .get(&key_invalid_qux)
            .expect("graph not found")
            .state
        {
            ApplicationState::Disabled { .. } => {}
            _ => panic!("graph_invalid_qux should be disabled"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_validate_graph_constraints_resources() -> Result<()> {
        use crate::data_model::{
            FunctionResources, GPUResources, GPU_MODEL_NVIDIA_A10, GPU_MODEL_NVIDIA_H100_80GB,
        };

        // Single catalog entry with specific capacities
        let catalog_entry = ExecutorCatalogEntry {
            name: "res".to_string(),
            cpu_cores: 2, // 2 cores
            memory_gb: 2, // 2 GB
            disk_gb: 2,   // 2 GB
            gpu_models: vec![GPU_MODEL_NVIDIA_A10.to_string()],
            labels: HashMap::new(),
        };

        let test_srv = TestService::new_with_executor_catalog(vec![catalog_entry]).await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Helper to build a graph with given resources applied to all nodes
        let build_graph = |name: &str, resources: FunctionResources| {
            let mut g = test_objects::mock_application();
            g.name = name.to_string();
            g.state = ApplicationState::Active;
            for node in g.nodes.values_mut() {
                node.resources = resources.clone();
            }
            g.start_fn.resources = resources;
            g
        };

        // Graphs per-resource scenario
        let graph_cpu_unsat = build_graph(
            "graph_cpu_unsat",
            FunctionResources {
                cpu_ms_per_sec: 3000, // needs 3 cores, catalog has 2
                memory_mb: 512,
                ephemeral_disk_mb: 512,
                gpu_configs: vec![],
            },
        );

        let graph_cpu_sat = build_graph(
            "graph_cpu_sat",
            FunctionResources {
                cpu_ms_per_sec: 2000, // exactly 2 cores
                memory_mb: 512,
                ephemeral_disk_mb: 512,
                gpu_configs: vec![],
            },
        );

        // Use very large values to exceed (catalog.memory_gb * 1024 * 1024) MB if that
        // logic is used
        let graph_mem_unsat = build_graph(
            "graph_mem_unsat",
            FunctionResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 3_000_000, // > 2,097,152 MB (2 TB) so should be unsatisfiable for 2 GB
                ephemeral_disk_mb: 512,
                gpu_configs: vec![],
            },
        );

        let graph_disk_unsat = build_graph(
            "graph_disk_unsat",
            FunctionResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 3_000_000, // > 2,097,152 MB (2 TB) so should be unsatisfiable for 2 GB
                gpu_configs: vec![],
            },
        );

        let graph_gpu_unsat = build_graph(
            "graph_gpu_unsat",
            FunctionResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 512,
                gpu_configs: vec![GPUResources {
                    count: 1,
                    model: GPU_MODEL_NVIDIA_H100_80GB.to_string(), // not in catalog
                }],
            },
        );

        let graph_gpu_sat = build_graph(
            "graph_gpu_sat",
            FunctionResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 512,
                gpu_configs: vec![GPUResources {
                    count: 1,
                    model: GPU_MODEL_NVIDIA_A10.to_string(), // in catalog
                }],
            },
        );

        // Persist graphs
        for application in [
            graph_cpu_unsat.clone(),
            graph_cpu_sat.clone(),
            graph_mem_unsat.clone(),
            graph_disk_unsat.clone(),
            graph_gpu_unsat.clone(),
            graph_gpu_sat.clone(),
        ] {
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateOrUpdateComputeGraph(Box::new(
                        CreateOrUpdateComputeGraphRequest {
                            namespace: TEST_NAMESPACE.to_string(),
                            application,
                            upgrade_requests_to_current_version: true,
                        },
                    )),
                })
                .await?;
        }

        // Validate constraints
        test_srv
            .service
            .graph_processor
            .validate_graph_constraints()
            .await?;
        test_srv.process_all_state_changes().await?;

        // Verify expected states
        let in_memory = indexify_state.in_memory_state.read().await;

        let assert_state = |name: &str, expect_active: bool| {
            let key = crate::data_model::Application::key_from(TEST_NAMESPACE, name);
            let graph = in_memory
                .applications
                .get(&key)
                .expect("compute graph not found");
            match (&graph.state, expect_active) {
                (ApplicationState::Active, true) => {}
                (ApplicationState::Disabled { .. }, false) => {}
                _ => panic!(
                    "compute graph '{}' unexpected state: {:?}",
                    name, graph.state
                ),
            }
        };

        // Unsatisfiable vs satisfiable across resources
        assert_state("graph_cpu_unsat", false);
        assert_state("graph_cpu_sat", true);
        assert_state("graph_mem_unsat", false);
        assert_state("graph_disk_unsat", false);
        assert_state("graph_gpu_unsat", false);
        assert_state("graph_gpu_sat", true);

        Ok(())
    }
}
