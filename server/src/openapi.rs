use crate::routes;
use serde_json::{Map, Value}; // modified import
use std::fs;
use utoipa::OpenApi;

pub fn remove_internal_routes(api_json: &str) -> String {
    // Parse the JSON into a serde_json::Value
    let mut json_value: Value = serde_json::from_str(api_json).expect("Invalid JSON format");

    // If there is a "paths" object, remove any route with "/internal"
    if let Some(paths) = json_value.get_mut("paths").and_then(|v| v.as_object_mut()) {
        paths.retain(|k, _| !k.contains("/internal"));
    }

    // Serialize the modified JSON back to a string
    serde_json::to_string(&json_value).expect("Failed to serialize JSON")
}

pub fn generate_openapi(public_docs: bool) {
    let api_docs = routes::ApiDoc::openapi().to_json().unwrap();
    // Replacing namespace to workflows
    let openapi = api_docs
        .replace("/namespaces/{namespace}", "/workflows")
        .replace("/namespaces", "/workflows"); // chained replacement

    // Remove any route that contains "/internal"
    let openapi_clean = remove_internal_routes(&openapi);

    fs::write("./openapi.json", &openapi_clean).unwrap();

    if public_docs {
        let mut json_value: Value =
            serde_json::from_str(&openapi_clean).expect("Failed to parse JSON");
        let root = json_value.as_object_mut().expect("Root is not an object");

        let mut new_root = Map::new();

        if let Some(openapi_value) = root.get("openapi") {
            new_root.insert("openapi".to_string(), openapi_value.clone());
        }

        let servers = vec![{
            let mut server_map = Map::new();
            server_map.insert(
                "url".to_string(),
                Value::String("https://api.tensorlake.ai/".to_string()),
            );
            Value::Object(server_map)
        }];
        new_root.insert("servers".to_string(), Value::Array(servers));

        let mut info_map = Map::new();
        info_map.insert(
            "title".to_string(),
            Value::String("Tensorlake API".to_string()),
        );
        info_map.insert(
            "description".to_string(),
            Value::String(
                "Tensorlake Cloud APIs for Document Ingestion and Serverless Workflows".to_string(),
            ),
        );

        if let Some(Value::Object(existing_info)) = root.get("info") {
            for (key, value) in existing_info.iter() {
                if key != "title" && key != "description" {
                    info_map.insert(key.clone(), value.clone());
                }
            }
        }
        new_root.insert("info".to_string(), Value::Object(info_map));

        let mut security_vec = Vec::new();
        let mut security_map = Map::new();
        security_map.insert("bearerAuth".to_string(), Value::Array(Vec::new()));
        security_vec.push(Value::Object(security_map));
        new_root.insert("security".to_string(), Value::Array(security_vec));

        for (key, value) in root.iter() {
            if key != "openapi" && key != "info" && key != "components" {
                new_root.insert(key.clone(), value.clone());
            }
        }

        let mut tags_vec = Vec::new();
        let mut tags_map = Map::new();
        tags_map.insert(
            "name".to_string(),
            Value::String("Tensorlake Cloud API".to_string()),
        );
        tags_map.insert(
            "description".to_string(),
            Value::String(
                "Tensorlake Cloud APIs for Document Ingestion and Serverless Workflows".to_string(),
            ),
        );
        tags_vec.push(Value::Object(tags_map));
        new_root.insert("tags".to_string(), Value::Array(tags_vec));

        let mut components_map =
            if let Some(Value::Object(existing_components)) = root.get("components") {
                existing_components.clone()
            } else {
                Map::new()
            };

        let mut security_schemes_map = Map::new();
        let mut bearer_auth_map = Map::new();
        bearer_auth_map.insert("type".to_string(), Value::String("http".to_string()));
        bearer_auth_map.insert("scheme".to_string(), Value::String("bearer".to_string()));
        security_schemes_map.insert("bearerAuth".to_string(), Value::Object(bearer_auth_map));

        let mut responses_map = Map::new();
        let mut unauthorized_error_map = Map::new();
        unauthorized_error_map.insert(
            "description".to_string(),
            Value::String("Access token is missing or invalid".to_string()),
        );
        responses_map.insert(
            "UnauthorizedError".to_string(),
            Value::Object(unauthorized_error_map),
        );

        components_map.insert(
            "securitySchemes".to_string(),
            Value::Object(security_schemes_map),
        );
        components_map.insert("responses".to_string(), Value::Object(responses_map));

        new_root.insert("components".to_string(), Value::Object(components_map));

        let updated_json =
            serde_json::to_string(&Value::Object(new_root)).expect("Failed to serialize JSON");
        fs::write("./openapi.json", updated_json).unwrap();
    }
}
