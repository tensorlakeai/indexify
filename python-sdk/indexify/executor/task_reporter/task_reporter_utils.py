from rich.panel import Panel

from indexify.executor.task_reporter.task_reporter import console


def _log_exception(task_outcome, e):
    console.print(
        Panel(
            f"Failed to report task {task_outcome.task.id}\n"
            f"Exception: {type(e).__name__}({e})\n"
            f"Retries: {task_outcome.reporting_retries}\n"
            "Retrying...",
            title="Reporting Error",
            border_style="error",
        )
    )


def _log(task_outcome):
    outcome = task_outcome.task_outcome
    style_outcome = (
        f"[bold red] {outcome} [/]"
        if "fail" in outcome
        else f"[bold green] {outcome} [/]"
    )
    console.print(
        Panel(
            f"Reporting outcome of task: {task_outcome.task.id}, function: {task_outcome.task.compute_fn}\n"
            f"Outcome: {style_outcome}\n"
            f"Num Fn Outputs: {len(task_outcome.outputs or [])}\n"
            f"Router Output: {task_outcome.router_output}\n"
            f"Retries: {task_outcome.reporting_retries}",
            title="Task Completion",
            border_style="info",
        )
    )
