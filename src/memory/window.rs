use crate::{ConversationHistory, ConversationHistoryError};

pub struct WindowConversationHistory {
    turns: Vec<String>,
    window_size: usize,
}

impl WindowConversationHistory{
    pub fn new(window_size: Option<usize>) -> Self {
        Self {
            turns: Vec::new(),
            window_size: window_size.unwrap_or(3),
        }
    }
}

impl ConversationHistory for WindowConversationHistory {
    fn add_turn(&mut self, _policy: String, turn: String) -> Result<(), ConversationHistoryError> {
        self.turns.push(turn);
        Ok(())
    }

    fn retrieve_history(&self, _policy: String, _query: String) -> Result<Vec<String>, ConversationHistoryError> {
        let total_turns = self.turns.len();
        if total_turns == 0 {
            return Err(ConversationHistoryError::InternalError(format!(
                "No turns found in the conversation history."
            )))
        } else if self.window_size <= total_turns {
            let start_index = total_turns - self.window_size;
            Ok(self.turns[start_index..].to_vec())
        } else {
            Ok(self.turns.clone())
        }
    }
}

// fn main() {
//     // Specify the window size
//     let window_size = 3;

//     // Create ConversationHistory object
//     let mut conversation = WindowConversationHistory::new(window_size);

//     // Add turns to the conversation
//     conversation.add_turn("User: Hello!".to_string());
//     conversation.add_turn("LLM: Hi, how can I assist you?".to_string());
//     conversation.add_turn("User: I have a question about product X.".to_string());
//     conversation.add_turn("LLM: Sure, what would you like to know?".to_string());
//     conversation.add_turn("User: How much does it cost?".to_string());
//     conversation.add_turn("LLM: The price of product X is $100.".to_string());

//     // Retrieve the conversation history
//     if let Some(history) = conversation.retrieve_history('Window', 'query') {
//         // Print the conversation history
//         for turn in history {
//             println!("{}", turn);
//         }
//     } else {
//         println!("Window size exceeds the total number of turns in the conversation.");
//     }
// }
