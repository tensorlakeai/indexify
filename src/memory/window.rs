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

    fn retrieve_history(&mut self, _policy: String, _query: String) -> Result<Vec<String>, ConversationHistoryError> {
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

mod tests {
    use crate::{memory::window::WindowConversationHistory, ConversationHistory, ConversationHistoryError};

    #[test]
    fn window_test() {
        let mut memory = WindowConversationHistory::new(Some(2));
        memory.add_turn("simple".to_string(), "Value 1".to_string()).map_err(|e| return ConversationHistoryError::InternalError(e.to_string())).ok();
        memory.add_turn("simple".to_string(), "Value 2".to_string()).map_err(|e| ConversationHistoryError::InternalError(e.to_string())).ok();
        memory.add_turn("simple".to_string(), "Value 3".to_string()).map_err(|e| ConversationHistoryError::InternalError(e.to_string())).ok();
        let result = memory.retrieve_history("window".to_string(), "sample_query".to_string()).map_err(|e| ConversationHistoryError::InternalError(e.to_string())).ok().unwrap();
        let target_result = ["Value 2".to_string(), "Value 3".to_string()].to_vec();
        assert_eq!(result, target_result);
    }
}
