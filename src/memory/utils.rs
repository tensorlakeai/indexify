use std::collections::HashMap;

use serde_json::json;

use crate::{persistence::Text, Message, SearchResult};

pub fn get_messages_from_texts(texts: Vec<Text>) -> Vec<Message> {
    let default_role = &"unknown".to_string();
    let messages: Vec<Message> = texts
        .iter()
        .map(|text| {
            let metadata = text.metadata.clone();
            Message {
                text: text.text.to_owned(),
                role: metadata.get("role").unwrap_or(default_role).into(),
                metadata: json!(metadata),
            }
        })
        .collect();
    messages
}

pub fn get_messages_from_search_results(results: Vec<SearchResult>) -> Vec<Message> {
    let default_role = &"unknown".to_string();
    let messages: Vec<Message> = results
        .iter()
        .map(|text| {
            let metadata: HashMap<String, String> =
                serde_json::from_value(text.metadata.to_owned()).unwrap();

            Message {
                text: text.texts.to_owned(),
                role: metadata.get("role").unwrap_or(default_role).into(),
                metadata: text.metadata.to_owned(),
            }
        })
        .collect();
    messages
}

pub fn get_texts_from_messages(session_id: &String, messages: Vec<Message>) -> Vec<Text> {
    let mut texts = vec![];
    for message in messages {
        let mut metadata = HashMap::from([
            ("role".to_string(), message.role),
            ("session_id".to_string(), session_id.to_string()),
        ]);
        let message_metadata: HashMap<String, String> =
            serde_json::from_value(message.metadata).unwrap();
        metadata.extend(message_metadata);
        texts.push(Text {
            text: message.text,
            metadata,
        });
    }
    texts
}
