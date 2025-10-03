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
