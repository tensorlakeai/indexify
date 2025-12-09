use crate::executor::executor_api::executor_api_pb::FunctionExecutorTerminationReason;
use crate::executor::{
    function_executor::FunctionExecutor, function_executor_controller::allocation::AllocationInfo,
};

pub enum Event {
    FunctionExecutorCreated {
        function_executor: Option<FunctionExecutor>,
        fe_termination_reason: Option<FunctionExecutorTerminationReason>,
    },
    FunctionExecutorTerminated {
        is_success: bool,
        fe_termination_reason: FunctionExecutorTerminationReason,
        allocation_ids_caused_termination: Vec<String>,
    },
    ShutdownInitiated,

    AllocationPreparationFinished {
        alloc_info: AllocationInfo,
        is_success: bool,
    },
    ScheduleAllocationExecution,
    AllocationExecutionFinished {
        alloc_info: AllocationInfo,
        function_executor_termination_reason: Option<FunctionExecutorTerminationReason>,
    },
    AllocationFinalizationFinished {
        alloc_info: AllocationInfo,
        is_success: bool,
    },
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::FunctionExecutorCreated { .. } => {
                write!(f, "Event(type=FunctionExecutorCreated)")
            }
            Event::FunctionExecutorTerminated {
                is_success,
                fe_termination_reason,
                allocation_ids_caused_termination,
            } => {
                write!(
                  f,
                  "Event(type=FunctionExecutorTerminated, is_success={}, fe_termination_reason={:?}, allocation_ids_caused_termination={:?})",
                  is_success, fe_termination_reason, allocation_ids_caused_termination
              )
            }
            Event::ShutdownInitiated => {
                write!(f, "Event(type=ShutdownInitiated)")
            }
            Event::AllocationPreparationFinished {
                alloc_info,
                is_success,
            } => {
                write!(
                  f,
                  "Event(type=AllocationPreparationFinished, function_call_id={:?}, allocation_id={:?}, is_success={})",
                  alloc_info.allocation.function_call_id,
                  alloc_info.allocation.allocation_id,
                  is_success
              )
            }
            Event::ScheduleAllocationExecution => {
                write!(f, "Event(type=ScheduleAllocationExecution)")
            }
            Event::AllocationExecutionFinished {
                alloc_info,
                function_executor_termination_reason,
            } => {
                let reason_str = function_executor_termination_reason
                    .as_ref()
                    .map(|r| format!("{:?}", r))
                    .unwrap_or_else(|| "None".to_string());
                write!(
                  f,
                  "Event(type=AllocationExecutionFinished, function_call_id={:?}, allocation_id={:?}, function_executor_termination_reason={})",
                  alloc_info.allocation.function_call_id,
                  alloc_info.allocation.allocation_id,
                  reason_str
              )
            }
            Event::AllocationFinalizationFinished {
                alloc_info,
                is_success,
            } => {
                write!(
                  f,
                  "Event(type=AllocationFinalizationFinished, function_call_id={:?}, allocation_id={:?}, is_success={})",
                  alloc_info.allocation.function_call_id,
                  alloc_info.allocation.allocation_id,
                  is_success
              )
            }
        }
    }
}
