# Executor Result Routing Bug: Stable `function_call_id` + Reschedule

## Confirmed invariants
- `function_run_id` and `function_call_id` are stable across attempts.
- Each retry attempt gets a new allocation ID.
- Allocation = one attempt instance of a function run.

## Why this is a bug
The current router registration path uses `register_if_absent` keyed by `function_call_id`.

Relevant code:
- `crates/server/src/executor_api/result_routing.rs` (`register_if_absent`, `handle_log_entry`)
- `crates/server/src/processor/function_run_processor.rs` (allocations inherit stable `function_call_id` from function run)

Given stable `function_call_id`, this breaks reschedules:
1. Attempt A registers route: `function_call_id -> (parent_allocation_id_A, executor_A)`.
2. Attempt A fails/disconnects before child result is routed.
3. Attempt B is scheduled with new allocation ID but same `function_call_id`.
4. `register_if_absent` skips updating route because key already exists.
5. Child result gets routed to stale executor/allocation and may be dropped.

This can manifest as hung requests or missing child results after executor disconnects.

## Recommended fix
1. Replace `register_if_absent` behavior with conditional overwrite:
   - Keep existing route only when it is the same active chain (`same executor_id` + `same parent_allocation_id`).
   - Overwrite when parent allocation or executor changed (reschedule/new attempt).
2. Add router cleanup on executor deregistration:
   - Purge all pending routes owned by that executor to avoid stale entries.
3. Keep tail-call behavior intact:
   - Preserve original function-call ID mapping for same parent chain.

## Regression tests to add
1. **Reschedule overwrite test**
   - Register route for attempt A.
   - Re-register same `function_call_id` for attempt B (new allocation/executor).
   - Verify router maps to attempt B.
2. **Stale executor purge test**
   - Register pending route for executor X.
   - Deregister executor X.
   - Verify pending entries for X are removed.
3. **Tail-call preserve test**
   - Verify downstream re-registration in same chain does not corrupt original function-call linkage.

## Expected outcome after fix
- Results for rescheduled attempts are routed to the latest owning parent allocation/executor.
- Disconnects are less likely to cause silent result loss.
- Fewer hangs from stale routing state under retry-heavy conditions.
