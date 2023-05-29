use uuid::Uuid;

use crate::{MemorySession, MemorySessionError};

pub struct IndefiniteMemorySession {
    session_id: Uuid,
    turns: Vec<String>,
}

impl IndefiniteMemorySession {
    pub fn new(session_id: Uuid) -> Self {
        Self {
            session_id,
            turns: Vec::new(),
        }
    }
}

impl MemorySession for IndefiniteMemorySession {
    fn add_turn(&mut self, turn: String) -> Result<(), MemorySessionError> {
        self.turns.push(turn);
        Ok(())
    }

    fn retrieve_history(&mut self, _query: String) -> Result<Vec<String>, MemorySessionError> {
        let total_turns = self.turns.len();
        if total_turns == 0 {
            return Err(MemorySessionError::InternalError(format!(
                "No records found in memory."
            )));
        } else {
            Ok(self.turns.clone())
        }
    }

    fn id(&self) -> Uuid {
        self.session_id
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{memory::indefinite::IndefiniteMemorySession, MemorySession, MemorySessionError};

    #[test]
    fn basic_test() {
        let session_id: Uuid = Uuid::new_v4();
        let mut memory = IndefiniteMemorySession::new(session_id);
        memory
            .add_turn("Value 1".to_string())
            .map_err(|e| return MemorySessionError::InternalError(e.to_string()))
            .ok();
        memory
            .add_turn("Value 2".to_string())
            .map_err(|e| MemorySessionError::InternalError(e.to_string()))
            .ok();
        let result = memory
            .retrieve_history("sample_query".to_string())
            .map_err(|e| MemorySessionError::InternalError(e.to_string()))
            .ok()
            .unwrap();
        let target_result = ["Value 1".to_string(), "Value 2".to_string()].to_vec();
        assert_eq!(result, target_result);
    }
}
