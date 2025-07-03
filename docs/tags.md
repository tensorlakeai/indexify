Unified tags for indexify / PE / FE stack. All events/metrics/spans should use these attribute names when relevant.

- graph - Name of graph.
- graph_version - Version of the graph
- invocation_id = ID of the invocation.
- fn - Name of the compute function
- allocation_id - ID of the allocation
- task_id - ID of the task.
- namespace - Name of the namespace.
- executor_sku - Type of executor (a100, h100, etc)
- executor_id - ID of the executor
- fn_executor_id - ID of the function executor.
- duration_sec - Duration of an operation in secs (use proper suffix if different unit of time is used)

Guiding principles:
- Consistent - Tag spelling and abreviations should be consistent (e.g. if we are shortening function to fn it should be done everywhere)
- Concise - Tags should be as short as possible while relaying what they are.

