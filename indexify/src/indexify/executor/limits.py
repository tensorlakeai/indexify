# An example of a function call of max supported size is a reduce call with 1000 inputs.
# It creates ~1 MB proto message. We limit the size of function call because Server is
# currently not capable of handling very large function calls due non linear algorithms
# used in it.
#
# TODO: Move these constants into Applications SDK and implement the validation in SDK
# (both FE and local runner) for best UX. The validation on Executor side is for service
# stability only.
MAX_FUNCTION_CALL_SIZE_MB: float = 1.0
MAX_FUNCTION_CALL_EXECUTION_PLAN_UPDATE_ITEMS_COUNT: int = 1000


# Server RPC settings. We retry for up to 5 minutes to allow for temporary Server availability issues
# due to i.e. short-ish network partitions, Server deployments, Server or underlying infra availability issues.
SERVER_RPC_TIMEOUT_SEC = 5
SERVER_RPC_MIN_BACKOFF_SEC = 2
SERVER_RPC_MAX_BACKOFF_SEC = 60  # 1 min
SERVER_RPC_MAX_RETRIES = (
    8  # exp backoffs: [2, 4, 8, 16, 32, 60, 60, 60] = 302 sec total (5 mins)
)
