use crate::{ConversationHistory, ConversationHistoryError};

pub struct SimpleConversationHistory {
    turns: Vec<String>,
}

impl SimpleConversationHistory {
    pub fn new() -> Self {
        Self {
            turns: Vec::new(),
        }
    }
}

impl ConversationHistory for SimpleConversationHistory {
    fn add_turn(&mut self, _policy: String, turn: String)-> Result<(), ConversationHistoryError> {
        self.turns.push(turn);
        Ok(())
    }

    fn retrieve_history(&self, _policy: String, _query: String) -> Result<Vec<String>, ConversationHistoryError>{
        let total_turns = self.turns.len();
        if total_turns == 0 {
            return Err(ConversationHistoryError::InternalError(format!(
                "No turns found in the conversation history."
            )))
        } else {
            Ok(self.turns.clone())
        }
    }
}

// fn main() {
//     // Create a new conversation
//     let mut conversation = SimpleConversationHistory::new();

//     // Add turns to the conversation
//     conversation.add_turn("Simple".to_string(), "User: Hello!".to_string());
//     conversation.add_turn("Simple".to_string(), "LLM: Hi, how can I assist you?".to_string());
//     conversation.add_turn("Simple".to_string(), "User: I have a question about product X.".to_string());

//     // Retrieve the conversation history
//     let history = conversation.retrieve_history("Simple".to_string(), "query".to_string());

//     // Print the conversation history
//     for turn in history {
//         println!("{}", turn);
//     }
// }
