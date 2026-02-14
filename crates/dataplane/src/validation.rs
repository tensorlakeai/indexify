//! Message validation for incoming server protos.
//!
//! Validates required fields on FunctionExecutorDescription, Allocation,
//! DataPayload, and FunctionRef before processing. Matches Python executor's
//! message_validators.py.

use proto_api::executor_api_pb::{
    Allocation,
    DataPayload,
    FunctionExecutorDescription,
    FunctionExecutorType,
    FunctionRef,
};

/// Validate a FunctionExecutorDescription. Returns an error message if invalid.
pub fn validate_fe_description(desc: &FunctionExecutorDescription) -> Result<(), String> {
    if desc.id.is_none() {
        return Err("missing id".into());
    }

    // Sandbox containers don't require function, application, or allocation fields
    let is_sandbox = desc
        .container_type
        .map(|t| FunctionExecutorType::try_from(t) == Ok(FunctionExecutorType::Sandbox))
        .unwrap_or(false);

    if is_sandbox {
        return Ok(());
    }

    let func = desc
        .function
        .as_ref()
        .ok_or_else(|| "missing function".to_string())?;
    validate_function_ref(func)?;

    if desc.initialization_timeout_ms.is_none() {
        return Err("missing initialization_timeout_ms".into());
    }
    let app = desc
        .application
        .as_ref()
        .ok_or_else(|| "missing application".to_string())?;
    validate_data_payload(app)?;

    if desc.max_concurrency.is_none() {
        return Err("missing max_concurrency".into());
    }
    if desc.allocation_timeout_ms.is_none() {
        return Err("missing allocation_timeout_ms".into());
    }

    Ok(())
}

/// Validate an Allocation. Returns an error message if invalid.
pub fn validate_allocation(alloc: &Allocation) -> Result<(), String> {
    let func = alloc
        .function
        .as_ref()
        .ok_or_else(|| "missing function".to_string())?;
    validate_function_ref(func)?;

    if alloc.allocation_id.is_none() {
        return Err("missing allocation_id".into());
    }
    if alloc.function_call_id.is_none() {
        return Err("missing function_call_id".into());
    }
    if alloc.request_id.is_none() {
        return Err("missing request_id".into());
    }
    if alloc.function_executor_id.is_none() {
        return Err("missing function_executor_id".into());
    }
    if alloc.request_data_payload_uri_prefix.is_none() {
        return Err("missing request_data_payload_uri_prefix".into());
    }
    if alloc.request_error_payload_uri_prefix.is_none() {
        return Err("missing request_error_payload_uri_prefix".into());
    }

    for (i, arg) in alloc.args.iter().enumerate() {
        validate_data_payload(arg).map_err(|e| format!("args[{}]: {}", i, e))?;
    }

    Ok(())
}

fn validate_function_ref(func: &FunctionRef) -> Result<(), String> {
    if func.namespace.is_none() {
        return Err("function: missing namespace".into());
    }
    if func.application_name.is_none() {
        return Err("function: missing application_name".into());
    }
    if func.function_name.is_none() {
        return Err("function: missing function_name".into());
    }
    Ok(())
}

fn validate_data_payload(dp: &DataPayload) -> Result<(), String> {
    if dp.uri.is_none() {
        return Err("data_payload: missing uri".into());
    }
    if dp.encoding.is_none() {
        return Err("data_payload: missing encoding".into());
    }
    if dp.encoding_version.is_none() {
        return Err("data_payload: missing encoding_version".into());
    }
    if dp.size.is_none() {
        return Err("data_payload: missing size".into());
    }
    if dp.sha256_hash.is_none() {
        return Err("data_payload: missing sha256_hash".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_function_ref() -> FunctionRef {
        FunctionRef {
            namespace: Some("ns".into()),
            application_name: Some("app".into()),
            function_name: Some("fn".into()),
            application_version: Some("v1".into()),
        }
    }

    fn valid_data_payload() -> DataPayload {
        DataPayload {
            uri: Some("file:///tmp/test".into()),
            encoding: Some(1),
            encoding_version: Some(0),
            offset: Some(0),
            size: Some(100),
            sha256_hash: Some("abc123".into()),
            ..Default::default()
        }
    }

    fn valid_fe_description() -> FunctionExecutorDescription {
        FunctionExecutorDescription {
            id: Some("fe-1".into()),
            function: Some(valid_function_ref()),
            initialization_timeout_ms: Some(30000),
            application: Some(valid_data_payload()),
            max_concurrency: Some(2),
            allocation_timeout_ms: Some(300000),
            ..Default::default()
        }
    }

    fn valid_allocation() -> Allocation {
        Allocation {
            function: Some(valid_function_ref()),
            allocation_id: Some("alloc-1".into()),
            function_call_id: Some("fc-1".into()),
            request_id: Some("req-1".into()),
            function_executor_id: Some("fe-1".into()),
            request_data_payload_uri_prefix: Some("file:///tmp".into()),
            request_error_payload_uri_prefix: Some("file:///tmp".into()),
            args: vec![valid_data_payload()],
            ..Default::default()
        }
    }

    #[test]
    fn test_valid_fe_description() {
        assert!(validate_fe_description(&valid_fe_description()).is_ok());
    }

    #[test]
    fn test_fe_description_missing_id() {
        let mut desc = valid_fe_description();
        desc.id = None;
        assert!(validate_fe_description(&desc).is_err());
    }

    #[test]
    fn test_fe_description_missing_function() {
        let mut desc = valid_fe_description();
        desc.function = None;
        assert!(validate_fe_description(&desc).is_err());
    }

    #[test]
    fn test_fe_description_missing_function_namespace() {
        let mut desc = valid_fe_description();
        desc.function.as_mut().unwrap().namespace = None;
        assert!(validate_fe_description(&desc).is_err());
    }

    #[test]
    fn test_valid_allocation() {
        assert!(validate_allocation(&valid_allocation()).is_ok());
    }

    #[test]
    fn test_allocation_missing_allocation_id() {
        let mut alloc = valid_allocation();
        alloc.allocation_id = None;
        assert!(validate_allocation(&alloc).is_err());
    }

    #[test]
    fn test_allocation_missing_arg_uri() {
        let mut alloc = valid_allocation();
        alloc.args[0].uri = None;
        assert!(validate_allocation(&alloc).is_err());
    }

    #[test]
    fn test_sandbox_fe_description_valid_without_application() {
        // Sandbox containers don't have application code payloads
        let desc = FunctionExecutorDescription {
            id: Some("sandbox-1".into()),
            container_type: Some(FunctionExecutorType::Sandbox.into()),
            ..Default::default()
        };
        assert!(validate_fe_description(&desc).is_ok());
    }

    #[test]
    fn test_sandbox_fe_description_requires_id() {
        let desc = FunctionExecutorDescription {
            container_type: Some(FunctionExecutorType::Sandbox.into()),
            ..Default::default()
        };
        assert!(validate_fe_description(&desc).is_err());
    }
}
