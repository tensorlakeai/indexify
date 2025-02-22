use std::{
    sync::{Arc, RwLock},
    vec,
};

use anyhow::{anyhow, Result};
use data_model::{ComputeGraphVersion, ExecutorId, ExecutorMetadata, Node, Task, TaskStatus};
use rand::seq::SliceRandom;
use state_store::{
    requests::{TaskPlacement, TaskPlacementDiagnostic},
    IndexifyState,
};
use tracing::{error, info, span, trace};

const MAX_ALLOCATIONS_PER_EXECUTOR: usize = 20;

pub struct ExecutorWithStats {
    pub executor: ExecutorMetadata,
    pub allocation_count: usize,
}

pub struct FilteredExecutors {
    pub executors: Vec<ExecutorId>,
    pub diagnostic_msgs: Vec<String>,
}

pub struct TaskPlacementResult {
    pub task_placements: Vec<TaskPlacement>,
    pub placement_diagnostics: Vec<TaskPlacementDiagnostic>,
}

struct ScheduleTaskResult {
    pub task_placements: Vec<TaskPlacement>,
    pub diagnostic_msgs: Vec<String>,
}

pub struct TaskAllocationProcessor {
    indexify_state: Arc<IndexifyState>,
    executors: Arc<RwLock<Vec<ExecutorMetadata>>>,
}

impl TaskAllocationProcessor {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        let executors = Arc::new(RwLock::new(Vec::new()));
        Self {
            indexify_state,
            executors,
        }
    }
}
impl TaskAllocationProcessor {
    pub fn schedule_unplaced_tasks(&self) -> Result<TaskPlacementResult> {
        if self.executors.read().unwrap().is_empty() {
            info!("no executors registered, skipping task allocation");
            return Ok(TaskPlacementResult {
                task_placements: vec![],
                placement_diagnostics: vec![],
            });
        }
        let mut executors_stats = self.get_executors_stats()?;
        if executors_stats.is_empty() {
            info!("no executors with allocation capacity, skipping task allocation");
            return Ok(TaskPlacementResult {
                task_placements: vec![],
                placement_diagnostics: vec![],
            });
        }
        let tasks = self.indexify_state.reader().unallocated_tasks()?;
        self.schedule_tasks(tasks, &mut executors_stats)
    }

    pub fn refresh_executors(&self) -> Result<()> {
        let all_executors = self.indexify_state.reader().get_all_executors()?;
        let mut executors = self.executors.write().unwrap();
        *executors = all_executors;
        Ok(())
    }

    fn get_executors_stats(&self) -> Result<Vec<ExecutorWithStats>> {
        let executors = self.executors.read().unwrap();
        let mut executors_stats = Vec::new();
        for executor in executors.iter() {
            let allocation_count = self
                .indexify_state
                .reader()
                .get_task_allocations(&executor.id)?
                .len();

            if allocation_count > MAX_ALLOCATIONS_PER_EXECUTOR {
                trace!(
                    "executor {} reached max allocations: {}",
                    executor.id,
                    allocation_count
                );
                continue;
            }

            executors_stats.push(ExecutorWithStats {
                executor: executor.clone(),
                allocation_count,
            });
        }
        Ok(executors_stats)
    }

    fn schedule_tasks(
        &self,
        tasks: Vec<Task>,
        executors_stats: &mut [ExecutorWithStats],
    ) -> Result<TaskPlacementResult> {
        let mut task_placements = Vec::new();
        let mut placement_diagnostics = Vec::new();

        for mut task in tasks {
            let span = span!(
                tracing::Level::INFO,
                "allocate_task",
                task_id = task.id.to_string(),
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                invocation_id = task.invocation_id.to_string(),
                compute_fn = task.compute_fn_name,
            );
            let _enter = span.enter();
            if task.outcome.is_terminal() {
                error!("task: {} already completed, skipping", task.id);
                continue;
            }

            // retrieve the executors that are not at max allocation count
            let executors: Vec<_> = executors_stats
                .iter()
                .filter(|s| {
                    if s.allocation_count < MAX_ALLOCATIONS_PER_EXECUTOR {
                        true
                    } else {
                        trace!(
                            "executor {} reached max allocations: {}",
                            s.executor.id,
                            s.allocation_count
                        );
                        false
                    }
                })
                .map(|s| s.executor.clone())
                .collect();

            if executors.is_empty() {
                info!("no more executors with allocation capacity, stopping task allocation");
                break;
            }

            match self.allocate_task(&executors, &mut task) {
                Ok(schedule_task_results) => {
                    task_placements.extend(schedule_task_results.task_placements.clone());
                    placement_diagnostics.extend(schedule_task_results.diagnostic_msgs.iter().map(
                        |msg| TaskPlacementDiagnostic {
                            task: task.clone(),
                            message: msg.clone(),
                        },
                    ));

                    schedule_task_results
                        .task_placements
                        .iter()
                        .for_each(|task_placement| {
                            if let Some(executor_stat) = executors_stats
                                .iter_mut()
                                .find(|stat| stat.executor.id == task_placement.executor)
                            {
                                executor_stat.allocation_count += 1;
                            }
                        });
                }
                Err(err) => {
                    error!("failed to allocate task, skipping: {:?}", err);
                }
            }
        }
        Ok(TaskPlacementResult {
            task_placements,
            placement_diagnostics,
        })
    }

    fn allocate_task(
        &self,
        executors: &[ExecutorMetadata],
        task: &mut Task,
    ) -> Result<ScheduleTaskResult> {
        let mut task_placements = Vec::new();
        let mut diagnostic_msgs = Vec::new();
        let compute_graph_version = self
            .indexify_state
            .reader()
            .get_compute_graph_version(
                &task.namespace,
                &task.compute_graph_name,
                &task.graph_version,
            )?
            .ok_or(anyhow!("compute graph not found"))?;
        let compute_fn = compute_graph_version
            .nodes
            .get(&task.compute_fn_name)
            .ok_or(anyhow!("compute fn not found"))?;
        let filtered_executors =
            self.filter_executors(executors, &compute_graph_version, &compute_fn)?;
        if !filtered_executors.diagnostic_msgs.is_empty() {
            diagnostic_msgs.extend(filtered_executors.diagnostic_msgs);
        }
        let executor_id = filtered_executors.executors.choose(&mut rand::thread_rng());
        if let Some(executor_id) = executor_id {
            info!(
                "allocating task {:?} to executor {:?}",
                task.id, executor_id
            );
            task.status = TaskStatus::Running;
            task_placements.push(TaskPlacement {
                task: task.clone(),
                executor: executor_id.clone(),
            });
        }
        Ok(ScheduleTaskResult {
            task_placements,
            diagnostic_msgs,
        })
    }

    fn filter_executors(
        &self,
        executors: &[ExecutorMetadata],
        compute_graph: &ComputeGraphVersion,
        node: &Node,
    ) -> Result<FilteredExecutors> {
        let mut filtered_executors = vec![];
        let mut diagnostic_msgs = vec![];

        for executor in executors.iter() {
            match executor.function_allowlist {
                Some(ref allowlist) => {
                    for func_uri in allowlist {
                        if func_matches(func_uri, compute_graph, node) {
                            filtered_executors.push(executor.id.clone());
                            break;
                        }
                    }
                }
                None => {
                    filtered_executors.push(executor.id.clone());
                }
            }
        }
        if !filtered_executors.is_empty() {
            diagnostic_msgs.clear();
        }
        Ok(FilteredExecutors {
            executors: filtered_executors,
            diagnostic_msgs,
        })
    }
}

fn func_matches(
    func_uri: &data_model::FunctionURI,
    compute_graph: &ComputeGraphVersion,
    node: &Node,
) -> bool {
    func_uri.compute_fn_name.eq(node.name()) &&
        func_uri
            .compute_graph_name
            .eq(&compute_graph.compute_graph_name) &&
        func_uri.version.as_ref().unwrap_or(&compute_graph.version) == &compute_graph.version &&
        func_uri.namespace.eq(&compute_graph.namespace)
}
