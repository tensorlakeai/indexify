export interface ComputeFn {
  name: string;
  fn_name: string;
  description: string;
  reducer: boolean;
  payload_encoder: string;
  image_name: string;
}

export interface DynamicRouter {
  name: string;
  source_fn: string;
  description: string;
  target_fns: string[];
  payload_encoder: string;
  image_name: string;
}

export type Node = 
  | { dynamic_router: DynamicRouter }
  | { compute_fn: ComputeFn };

export interface ComputeGraph {
  name: string;
  namespace: string;
  description: string;
  start_node: Node;
  nodes: Record<string, Node>;
  edges: Record<string, string[]>;
  created_at?: number;
}

export interface ComputeGraphsList {
  compute_graphs: ComputeGraph[];
  cursor?: string | null;
}

export interface CreateNamespace {
  name: string;
}

export interface DataObject {
  id: string;
  payload_size: number;
  payload_sha_256: string;
}

export interface GraphInvocations {
  invocations: DataObject[];
  cursor?: string | null;
}

export interface IndexifyAPIError {
  status_code: number;
  message: string;
}

export interface InvocationResult {
  outputs: Record<string, DataObject[]>;
  cursor?: string | null;
}

export interface Namespace {
  name: string;
  created_at: number;
}

export interface NamespaceList {
  namespaces: Namespace[];
}

export type TaskOutcome = "Unknown" | "Success" | "Failure";

export type GraphVersion = string;

export interface Task {
  id: string;
  namespace: string;
  compute_fn: string;
  compute_graph: string;
  invocation_id: string;
  input_key: string;
  outcome: TaskOutcome;
  graph_version: GraphVersion;
  reducer_output_id?: string | null;
}

export interface Tasks {
  tasks: Task[];
  cursor?: string | null;
}

export interface ComputeGraphCreateType {
  compute_graph: ComputeGraph;
  code: string;
}

export interface ExecutorMetadata {
  id: string;
  addr: string;
  image_name: string;
  labels: Record<string, any>;
}
