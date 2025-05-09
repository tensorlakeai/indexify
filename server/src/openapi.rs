use std::fs;

use serde_yaml::{Mapping, Value};

pub fn generate_cloud_openapi(api_docs_yaml: String) {
    let openapi = api_docs_yaml.replace("/namespaces/{namespace}", "/workflows");

    let mut yaml_value: Value = serde_yaml::from_str(&openapi).expect("Failed to parse YAML");
    let root = yaml_value.as_mapping_mut().expect("Root is not a mapping");

    let mut new_root = Mapping::new();

    if let Some(v) = root.get(&Value::String("openapi".into())) {
        new_root.insert(Value::String("openapi".into()), v.clone());
    }

    let servers = vec![{
        let mut map = Mapping::new();
        map.insert("url".into(), "https://api.tensorlake.ai/".into());
        Value::Mapping(map)
    }];
    new_root.insert("servers".into(), Value::Sequence(servers));

    let mut info_map = Mapping::new();
    info_map.insert("title".into(), "Tensorlake API".into());
    info_map.insert(
        "description".into(),
        "Tensorlake Cloud APIs for Document Ingestion and Serverless Workflows".into(),
    );
    if let Some(Value::Mapping(existing)) = root.get(&Value::String("info".into())) {
        for (k, v) in existing {
            if k.as_str() != Some("title") && k.as_str() != Some("description") {
                info_map.insert(k.clone(), v.clone());
            }
        }
    }
    new_root.insert("info".into(), Value::Mapping(info_map));

    let mut sec_map = Mapping::new();
    sec_map.insert("bearerAuth".into(), Value::Sequence(vec![]));
    new_root.insert(
        "security".into(),
        Value::Sequence(vec![Value::Mapping(sec_map)]),
    );

    for (k, v) in root.iter() {
        match k.as_str() {
            Some("openapi") | Some("info") | Some("components") | Some("paths") => {}
            _ => {
                new_root.insert(k.clone(), v.clone());
            }
        }
    }

    if let Some(Value::Mapping(orig_paths)) = root.get(&Value::String("paths".into())) {
        let mut filtered = Mapping::new();
        for (path_key, path_value) in orig_paths {
            if let Some(s) = path_key.as_str() {
                if s.starts_with("/internal") || s == "/namespaces" {
                    // skip
                    continue;
                }
            }
            filtered.insert(path_key.clone(), path_value.clone());
        }
        new_root.insert("paths".into(), Value::Mapping(filtered));
    }

    let mut tags_map = Mapping::new();
    tags_map.insert("name".into(), "Tensorlake Cloud API".into());
    tags_map.insert(
        "description".into(),
        "Tensorlake Cloud APIs for Serverless Workflows".into(),
    );
    new_root.insert(
        "tags".into(),
        Value::Sequence(vec![Value::Mapping(tags_map)]),
    );

    let mut components_map =
        if let Some(Value::Mapping(existing)) = root.get(&Value::String("components".into())) {
            existing.clone()
        } else {
            Mapping::new()
        };

    let mut bearer = Mapping::new();
    bearer.insert("type".into(), "http".into());
    bearer.insert("scheme".into(), "bearer".into());
    let mut sec_schemes = Mapping::new();
    sec_schemes.insert("bearerAuth".into(), Value::Mapping(bearer));
    components_map.insert("securitySchemes".into(), Value::Mapping(sec_schemes));

    let mut unauth = Mapping::new();
    unauth.insert(
        "description".into(),
        "Access token is missing or invalid".into(),
    );
    let mut responses = Mapping::new();
    responses.insert("UnauthorizedError".into(), Value::Mapping(unauth));
    components_map.insert("responses".into(), Value::Mapping(responses));

    new_root.insert("components".into(), Value::Mapping(components_map));

    let updated_yaml =
        serde_yaml::to_string(&Value::Mapping(new_root)).expect("Failed to serialize YAML");
    fs::write("./openapi.yaml", updated_yaml).unwrap();
}
