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
