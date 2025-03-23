use std::sync::Arc;

use anyhow::Result;
use data_model::{
    test_objects::tests::{
        self,
        mock_invocation_ctx,
        mock_invocation_payload,
        mock_invocation_payload_graph_b,
        mock_node_fn_output_fn_a,
        mock_node_router_output_x,
        TEST_EXECUTOR_ID,
        TEST_NAMESPACE,
    },
    ExecutorId,
    NodeOutput,
    Task,
    TaskOutcome,
    TaskStatus,
};

use crate::{
    requests::{
        CreateOrUpdateComputeGraphRequest,
        IngestTaskOutputsRequest,
        InvokeComputeGraphRequest,
        RequestPayload,
        StateMachineUpdateRequest,
    },
    IndexifyState,
};

pub struct TestStateStore {
    pub indexify_state: Arc<IndexifyState>,
}

impl TestStateStore {
    pub async fn new() -> Result<TestStateStore> {
        let temp_dir = tempfile::tempdir()?;
        let indexify_state = IndexifyState::new(temp_dir.path().join("state")).await?;
        Ok(TestStateStore { indexify_state })
    }

    pub async fn with_simple_graph(&self) -> String {
        with_simple_graph(&self.indexify_state).await
    }

    pub async fn with_router_graph(&self) -> String {
        with_router_graph(&self.indexify_state).await
    }

    pub async fn with_reducer_graph(&self) -> String {
        with_reducer_graph(&self.indexify_state).await
    }

    pub async fn finalize_task(
        &self,
        task: &Task,
        num_outputs: usize,
        task_outcome: TaskOutcome,
        reducer: bool,
    ) -> Result<()> {
        finalize_task(
            &self.indexify_state,
            task,
            num_outputs,
            task_outcome,
            reducer,
        )
        .await
    }

    pub async fn finalize_task_graph_b(&self, invocation_id: &str, task: &Task) -> Result<()> {
        finalize_task_graph_b(&self.indexify_state, invocation_id, task).await
    }

    pub async fn finalize_router_x(&self, invocation_id: &str, task: &Task) -> Result<()> {
        finalize_router_x(&self.indexify_state, invocation_id, task).await
    }
}

pub async fn with_simple_graph(indexify_state: &IndexifyState) -> String {
    let cg_request = CreateOrUpdateComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph: tests::mock_graph_a("image_hash".to_string()),
        upgrade_tasks_to_current_version: true,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
            processed_state_changes: vec![],
        })
        .await
        .unwrap();
    let invocation_payload = mock_invocation_payload();
    let ctx = mock_invocation_ctx(
        TEST_NAMESPACE,
        &tests::mock_graph_a("image_hash".to_string()),
        &invocation_payload,
    );
    let request = InvokeComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph_name: "graph_A".to_string(),
        invocation_payload: invocation_payload.clone(),
        ctx,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::InvokeComputeGraph(request),
            processed_state_changes: vec![],
        })
        .await
        .unwrap();
    invocation_payload.id
}

pub async fn with_router_graph(indexify_state: &IndexifyState) -> String {
    let cg_request = CreateOrUpdateComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph: tests::mock_graph_b(),
        upgrade_tasks_to_current_version: false,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
            processed_state_changes: vec![],
        })
        .await
        .unwrap();

    let invocation_payload = mock_invocation_payload_graph_b();
    let ctx = mock_invocation_ctx(TEST_NAMESPACE, &tests::mock_graph_b(), &invocation_payload);
    let request = InvokeComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph_name: "graph_B".to_string(),
        invocation_payload: invocation_payload.clone(),
        ctx,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::InvokeComputeGraph(request),
            processed_state_changes: vec![],
        })
        .await
        .unwrap();
    invocation_payload.id
}

pub async fn with_reducer_graph(indexify_state: &IndexifyState) -> String {
    let cg_request = CreateOrUpdateComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph: tests::mock_graph_with_reducer(),
        upgrade_tasks_to_current_version: false,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
            processed_state_changes: vec![],
        })
        .await
        .unwrap();

    let invocation_payload = mock_invocation_payload_graph_b();
    let ctx = mock_invocation_ctx(
        TEST_NAMESPACE,
        &tests::mock_graph_with_reducer(),
        &invocation_payload,
    );
    let request = InvokeComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph_name: "graph_R".to_string(),
        invocation_payload: invocation_payload.clone(),
        ctx,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::InvokeComputeGraph(request),
            processed_state_changes: vec![],
        })
        .await
        .unwrap();
    invocation_payload.id
}

pub async fn finalize_task(
    indexify_state: &IndexifyState,
    task: &Task,
    num_outputs: usize,
    task_outcome: TaskOutcome,
    reducer: bool,
) -> Result<()> {
    let compute_fn_for_reducer = if reducer {
        Some(task.compute_fn_name.to_string())
    } else {
        None
    };
    let node_outputs: Vec<NodeOutput> = (0..num_outputs)
        .map(|_| {
            mock_node_fn_output_fn_a(
                &task.invocation_id,
                &task.compute_graph_name,
                compute_fn_for_reducer.clone(),
            )
        })
        .collect();

    let mut task = task.clone();
    task.outcome = task_outcome.clone();
    task.status = TaskStatus::Completed;

    let request = IngestTaskOutputsRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph: task.compute_graph_name.to_string(),
        compute_fn: task.compute_fn_name.to_string(),
        invocation_id: task.invocation_id.to_string(),
        task: task.clone(),
        node_outputs,
        executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
    };

    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::IngestTaskOutputs(request),
            processed_state_changes: vec![],
        })
        .await
}

pub async fn finalize_task_graph_b(
    indexify_state: &IndexifyState,
    invocation_id: &str,
    task: &Task,
) -> Result<()> {
    let mut task = task.clone();
    task.outcome = TaskOutcome::Success;
    task.status = TaskStatus::Completed;

    let request = IngestTaskOutputsRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph: "graph_B".to_string(),
        compute_fn: "fn_a".to_string(),
        invocation_id: invocation_id.to_string(),
        task: task.clone(),
        node_outputs: vec![mock_node_fn_output_fn_a(&invocation_id, "graph_B", None)],
        executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::IngestTaskOutputs(request),
            processed_state_changes: vec![],
        })
        .await
}

pub async fn finalize_router_x(
    indexify_state: &IndexifyState,
    invocation_id: &str,
    task: &Task,
) -> Result<()> {
    let mut task = task.clone();
    task.outcome = TaskOutcome::Success;
    task.status = TaskStatus::Completed;

    let request = IngestTaskOutputsRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph: "graph_B".to_string(),
        compute_fn: "router_x".to_string(),
        invocation_id: invocation_id.to_string(),
        task: task.clone(),
        node_outputs: vec![mock_node_router_output_x(&invocation_id, "graph_B")],
        executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::IngestTaskOutputs(request),
            processed_state_changes: vec![],
        })
        .await
}
