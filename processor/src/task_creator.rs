                if let Some(invocation_error_payload) = node_output.invocation_error_payload.clone()
                {
                    invocation_ctx.invocation_error = Some(GraphInvocationError {
                        function_name: task.compute_fn_name.clone(),
                        payload: invocation_error_payload,
                    });
                }
            }
            invocation_ctx.failure_reason = task.failure_reason.unwrap_or_default().into();
            invocation_ctx.complete_invocation(true, GraphInvocationOutcome::Failure);
            return Ok(TaskCreationResult {
                invocation_ctx: Some(invocation_ctx),
            });

            if compute_graph_version.should_retry_task(&task) && allocation.is_retriable()
            {
                task.status = TaskStatus::Pending;
                task.attempt_number += 1;
            }