// ----------------------------------------------
// Namespaces
// ----------------------------------------------

export interface Namespace {
  name: string
  created_at: number
}

// ----------------------------------------------
// Applications
// ----------------------------------------------

export interface ApplicationsList {
  applications: Application[]
  cursor?: string | null
}

export interface Application {
  created_at: number
  description: string
  entrypoint: EntryPointManifest
  functions: Record<string, ApplicationFunction>
  name: string
  namespace: string
  tags: Record<string, string>
  tombstoned: boolean
  version: string
}

export interface ApplicationFunction {
  cache_key?: string | null
  description: string
  initialization_timeout_sec: number
  max_concurrency: number
  name: string
  parameters: Parameter[]
  placement_constraints: PlacementConstraints
  resources: NodeResources
  retry_policy: NodeRetryPolicy
  return_type?: string
  secret_names: string[]
  timeout_sec: number
}
export interface EntryPointManifest {
  function_name: string
  input_serializer: string
  output_serializer: string
  output_type_hints_base64: string
}

export interface GPURequirement {
  count: number
  model: string
}

export interface NodeResources {
  cpus: number
  ephemeral_disk_mb: number
  gpus: GPURequirement[]
  memory_mb: number
}

export interface NodeRetryPolicy {
  delay_multiplier: number
  initial_delay_sec: number
  max_delay_sec: number
  max_retries: number
}

export interface Parameter {
  data_type: string
  description: string
  name: string
  required: boolean
}

export interface PlacementConstraints {
  filter_expressions: string[]
}

// ----------------------------------------------
// Requests
// ----------------------------------------------

export interface DataPayload {
  id: string
  path: string
  size: number
  sha256_hash: string
}

export interface GraphRequest {
  id: string
  application_version: string
  created_at: number
  function_runs: FunctionRun[]
  outcome?: RequestOutcome
  output?: DataPayload
  request_error?: RequestError
}

export interface GraphRequests {
  requests: ShallowGraphRequest[]
  prev_cursor?: string | null
  next_cursor?: string | null
}

export interface ShallowGraphRequest {
  id: string
  outcome?: RequestOutcome
  created_at: number
}

export type RequestError = {
  function_name: string
  message: string
}

export type RequestOutcome =
  | 'undefined'
  | 'success'
  | { failure: RequestFailureReason }

export type RequestFailureReason =
  | 'unknown'
  | 'internalerror'
  | 'functionerror'
  | 'requesterror'
  | 'nextfunctionnotfound'
  | 'constraintunsatisfiable'

export type RequestStatus = 'pending' | 'running' | 'completed'

// ----------------------------------------------
// Function Runs
// ----------------------------------------------

export interface FunctionRuns {
  function_runs: FunctionRun[]
  cursor?: string | null
}

export interface FunctionRun {
  id: string
  function_name: string
  status: TaskStatus
  outcome?: TaskOutcome
  application_version: GraphVersion
  allocations?: Allocation[]
  created_at: number
}

export type GraphVersion = string

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

// ----------------------------------------------
// Executors
// ----------------------------------------------

export interface ExecutorMetadata {
  id: string
  executor_version: string
  function_allowlist: FunctionAllowlistEntry[] | null
  addr: string
  labels: Record<string, string>
  function_executors: FunctionExecutorMetadata[]
  server_only_function_executors: FunctionExecutorMetadata[]
  host_resources: HostResources
  free_resources: HostResources
  state: string
  tombstoned: boolean
  state_hash: string
  clock: number
}

interface FunctionAllowlistEntry {
  compute_fn: string
  compute_graph: string
  namespace: string
  version: string | null
}

export interface FunctionExecutorMetadata {
  id: string
  namespace: string
  compute_graph_name: string
  compute_fn_name: string
  version: string
  state: string
  desired_state: string
}

export interface HostResources {
  cpu_count: number
  memory_bytes: number
  disk_bytes: number
  gpu: GPUResources | null
}

export interface GPUResources {
  count: number
  model: string
}
