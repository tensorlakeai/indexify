use crate::{MemorySession, MemorySessionError};

use uuid::Uuid;

pub struct WindowMemorySession {
    session_id: Uuid,
    turns: Vec<String>,
    window_size: usize,
}

impl WindowMemorySession {
    pub fn new(session_id: Uuid, window_size: Option<usize>) -> Self {
        Self {
            session_id,
            turns: Vec::new(),
            window_size: window_size.unwrap_or(3),
        }
    }
}

impl MemorySession for WindowMemorySession {
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
        } else if self.window_size <= total_turns {
            let start_index = total_turns - self.window_size;
            Ok(self.turns[start_index..].to_vec())
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

    use crate::{memory::window::WindowMemorySession, MemorySession, MemorySessionError};

    #[test]
    fn simple_add_retrieve_test() {
        let session_id: Uuid = Uuid::new_v4();
        let mut memory = WindowMemorySession::new(session_id, Some(2));
        memory
            .add_turn("Value 1".to_string())
            .map_err(|e| return MemorySessionError::InternalError(e.to_string()))
            .ok();
        memory
            .add_turn("Value 2".to_string())
            .map_err(|e| MemorySessionError::InternalError(e.to_string()))
            .ok();
        memory
            .add_turn("Value 3".to_string())
            .map_err(|e| MemorySessionError::InternalError(e.to_string()))
            .ok();
        let result = memory
            .retrieve_history("sample_query".to_string())
            .map_err(|e| MemorySessionError::InternalError(e.to_string()))
            .ok()
            .unwrap();
        assert_eq!(
            result,
            ["Value 2".to_string(), "Value 3".to_string()].to_vec()
        );
        assert_eq!(memory.window_size, 2);
        assert_eq!(memory.id(), session_id);
    }
}
