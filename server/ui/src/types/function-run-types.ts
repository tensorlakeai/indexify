export interface Allocation {
  id: string
  function_name: string
  executor_id: string
  function_executor_id: string
  created_at: number
  outcome: TaskOutcome
  attempt_number: number
  execution_duration_ms: number | null
}

export type TaskOutcome = 'Unknown' | 'Success' | 'Failure'
export type TaskStatus = 'pending' | 'running' | 'completed'
