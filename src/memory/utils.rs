use std::collections::HashMap;

use serde_json::json;

use crate::{persistence::Text, Message};

pub fn get_messages_from_texts(texts: Vec<Text>) -> Vec<Message> {
    let default_role = &"unknown".to_string();
    let messages: Vec<Message> = texts
        .iter()
        .map(|text| Message {
            text: text.text.to_owned(),
            role: text
                .metadata
                .clone()
                .get("role")
                .unwrap_or(&serde_json::Value::String(default_role.to_owned()))
                .as_str()
                .unwrap()
                .to_owned(),
            metadata: text.metadata.clone(),
        })
        .collect();
    messages
}

pub fn get_texts_from_messages(session_id: &str, messages: Vec<Message>) -> Vec<Text> {
    let mut texts = vec![];
    for message in messages {
        let mut metadata: HashMap<String, serde_json::Value> = HashMap::from([
            ("role".to_string(), json!(message.role)),
            ("session_id".to_string(), json!(session_id.to_string())),
        ]);
        metadata.extend(message.metadata);
        texts.push(Text {
            text: message.text,
            metadata,
        });
    }
    texts
}
