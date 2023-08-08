use std::collections::HashMap;

use serde_json::json;

use crate::{persistence::Text, Message};

fn get_role(metadata: &HashMap<String, serde_json::Value>) -> String {
    let default_role = &"unknown".to_string();
    metadata
        .get("role")
        .and_then(|r| r.as_str())
        .unwrap_or(default_role)
        .to_string()
}

pub fn get_messages_from_texts(texts: Vec<Text>) -> Vec<Message> {
    let messages: Vec<Message> = texts
        .iter()
        .map(|text| {
            let role = get_role(&text.metadata);
            Message::new(&text.text, &role, text.metadata.clone())
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
            id: message.id,
            metadata,
        });
    }
    texts
}
