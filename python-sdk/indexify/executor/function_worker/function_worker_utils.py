import os

import nanoid


def get_optimal_process_count():
    """
    Returns a reasonable number of processes based on CPU cores.
    Generally CPU cores - 1 to leave one core for the OS/other tasks.
    """
    return max(os.cpu_count() - 1, 1)

def job_generator() -> str:
    """
    Generates job ID
    """
    return nanoid.generate()
