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

    fn retrieve_history(&mut self, _policy: String, _query: String) -> Result<Vec<String>, ConversationHistoryError>{
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

mod tests {
    use crate::{memory::simple::SimpleConversationHistory, ConversationHistory, ConversationHistoryError};

    #[test]
    fn basic_test() {
        let mut memory = SimpleConversationHistory::new();
        memory.add_turn("simple".to_string(), "Value 1".to_string()).map_err(|e| return ConversationHistoryError::InternalError(e.to_string())).ok();
        memory.add_turn("simple".to_string(), "Value 2".to_string()).map_err(|e| ConversationHistoryError::InternalError(e.to_string())).ok();
        let result = memory.retrieve_history("simple".to_string(), "sample_query".to_string()).map_err(|e| ConversationHistoryError::InternalError(e.to_string())).ok().unwrap();
        let target_result = ["Value 1".to_string(), "Value 2".to_string()].to_vec();
        assert_eq!(result, target_result);
    }
}
