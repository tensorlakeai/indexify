
#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::collections::HashMap;

    use crate::{
        config::ExecutorCatalogEntry,
        data_model::{
            filter::{Expression, LabelsFilter, Operator}, test_objects::tests::{self as test_objects, TEST_NAMESPACE}, ComputeGraphState
        },
        state_store::requests::{CreateOrUpdateComputeGraphRequest, RequestPayload, StateMachineUpdateRequest},
        testing::TestService,
    };

    #[tokio::test]
    async fn test_invocation_with_unsatisfiable_and_satisfiable_labels() -> Result<()> {
        // Step 1: Create an executor catalog entry with label foo=bar
        let mut labels = HashMap::new();
        labels.insert("foo".to_string(), "bar".to_string());

        let catalog_entry = ExecutorCatalogEntry {
            name: "custom".to_string(), // name is unused when labels are populated
            regions: vec![],              // regions unused
            labels,
        };

        // Initialize the test service with the catalog
        let test_srv = TestService::new_with_executor_catalog(vec![catalog_entry]).await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Step 2: Build a compute graph whose functions require foo==baz (unsatisfiable)
        let mut compute_graph = test_objects::test_graph_a("image_hash".to_string());
        compute_graph.name = "graph_unsatisfiable".to_string();
        compute_graph.state = ComputeGraphState::Active;

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
            compute_graph: compute_graph.clone(),
            upgrade_tasks_to_current_version: true,
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                processed_state_changes: vec![],
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
        let key = crate::data_model::ComputeGraph::key_from(TEST_NAMESPACE, "graph_unsatisfiable");
        let stored_graph = in_memory
            .compute_graphs
            .get(&key)
            .expect("compute graph not found in state");

        println!("stored_graph: {:?}", stored_graph);

        match &stored_graph.state {
            ComputeGraphState::Disabled { .. } => {},
            _ => panic!("Compute graph should be disabled due to unsatisfiable constraints"),
        }

        // Release the read guard before further updates
        drop(in_memory);

        // Step 6: Build a compute graph whose functions require foo==bar (satisfiable)
        let mut sat_compute_graph = test_objects::test_graph_a("image_hash".to_string());
        sat_compute_graph.name = "graph_satisfiable".to_string();
        sat_compute_graph.state = ComputeGraphState::Active;

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
            compute_graph: sat_compute_graph.clone(),
            upgrade_tasks_to_current_version: true,
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(sat_cg_request),
                processed_state_changes: vec![],
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
        let sat_key = crate::data_model::ComputeGraph::key_from(TEST_NAMESPACE, "graph_satisfiable");
        let sat_stored_graph = in_memory
            .compute_graphs
            .get(&sat_key)
            .expect("satisfiable compute graph not found in state");

        match &sat_stored_graph.state {
            ComputeGraphState::Active => {},
            _ => panic!("Compute graph should remain active due to satisfiable constraints"),
        }

        Ok(())
    }
}

