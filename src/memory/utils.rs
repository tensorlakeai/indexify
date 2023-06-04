use std::collections::HashMap;

use serde_json::json;
use uuid::Uuid;

use crate::{persistence::Text, Message, SearchResult};

pub fn get_messages_from_texts(texts: Vec<Text>) -> Vec<Message> {
    let messages: Vec<Message> = texts
        .iter()
        .map(|text| {
            let mut metadata = text.metadata.to_owned();
            let role = metadata.remove("role").unwrap();
            Message {
                text: text.text.to_owned(),
                role: role,
                metadata: json!(metadata),
            }
        })
        .collect();
    return messages;
}

pub fn get_messages_from_search_results(results: Vec<SearchResult>) -> Vec<Message> {
    let messages: Vec<Message> = results
        .iter()
        .map(|text| {
            let mut metadata: HashMap<String, String> =
                serde_json::from_value(text.metadata.to_owned()).unwrap();
            Message {
                text: text.texts.to_owned(),
                role: metadata.remove("role").unwrap(),
                metadata: text.metadata.to_owned(),
            }
        })
        .collect();
    return messages;
}

pub fn get_texts_from_messages(session_id: Uuid, messages: Vec<Message>) -> Vec<Text> {
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
            metadata: metadata,
        });
    }
    return texts;
}
