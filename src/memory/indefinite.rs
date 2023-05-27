use crate::{MemorySession, MemorySessionError};

pub struct IndefiniteMemorySession {
    turns: Vec<String>,
}

impl IndefiniteMemorySession {
    pub fn new() -> Self {
        Self {
            turns: Vec::new(),
        }
    }
}

impl MemorySession for IndefiniteMemorySession {
    fn add_turn(&mut self, turn: String)-> Result<(), MemorySessionError> {
        self.turns.push(turn);
        Ok(())
    }

    fn retrieve_history(&mut self, _query: String) -> Result<Vec<String>, MemorySessionError>{
        let total_turns = self.turns.len();
        if total_turns == 0 {
            return Err(MemorySessionError::InternalError(format!(
                "No turns found in the conversation history."
            )))
        } else {
            Ok(self.turns.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{memory::indefinite::IndefiniteMemorySession, MemorySession, MemorySessionError};

    #[test]
    fn basic_test() {
        let mut memory = IndefiniteMemorySession::new();
        memory.add_turn("Value 1".to_string()).map_err(|e| return MemorySessionError::InternalError(e.to_string())).ok();
        memory.add_turn("Value 2".to_string()).map_err(|e| MemorySessionError::InternalError(e.to_string())).ok();
        let result = memory.retrieve_history("sample_query".to_string()).map_err(|e| MemorySessionError::InternalError(e.to_string())).ok().unwrap();
        let target_result = ["Value 1".to_string(), "Value 2".to_string()].to_vec();
        assert_eq!(result, target_result);
    }
}
