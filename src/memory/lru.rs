use std::collections::{HashMap, VecDeque};

use uuid::Uuid;

use crate::{MemorySession, MemorySessionError};

pub struct LRUCache {
    session_id: Uuid,
    capacity: usize,
    cache: HashMap<String, String>,
    history: VecDeque<String>,
}

impl LRUCache {
    pub fn new(session_id: Uuid, capacity: Option<usize>) -> LRUCache {
        Self {
            session_id,
            capacity: capacity.unwrap_or(12),
            cache: HashMap::new(),
            history: VecDeque::new(),
        }
    }

    fn remove_oldest_entry(&mut self) {
        if let Some(key) = self.history.pop_front() {
            self.cache.remove(&key);
        }
    }

    /// TODO: Implement calculate_similarity
    fn calculate_similarity(_query: &str, _turn: &str) -> f64 {
        0.0
    }

    fn get_size(&self) -> usize {
        self.history.len()
    }
}

impl MemorySession for LRUCache {
    fn add_turn(&mut self, turn: String) -> Result<(), MemorySessionError> {
        if self.get_size() >= self.capacity {
            self.remove_oldest_entry();
        }

        self.history.push_back(turn.clone());
        self.cache.insert(turn.clone(), turn);
        Ok(())
    }

    fn retrieve_history(&mut self, query: String) -> Result<Vec<String>, MemorySessionError> {
        let mut history_scores: Vec<(String, f64)> = self
            .history
            .iter()
            .map(|turn| (turn.clone(), LRUCache::calculate_similarity(&query, turn)))
            .collect();

        history_scores.sort_by(|(_, score1), (_, score2)| score2.partial_cmp(score1).unwrap());

        let relevant_history: Vec<String> =
            history_scores.into_iter().map(|(turn, _)| turn).collect();
        Ok(relevant_history)
    }

    fn id(&self) -> Uuid {
        self.session_id
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{memory::lru::LRUCache, MemorySession, MemorySessionError};

    #[test]
    fn test_add_turn() {
        let session_id: Uuid = Uuid::new_v4();
        let mut cache = LRUCache::new(session_id, Some(2));
        cache
            .add_turn("Value 1".to_string())
            .map_err(|e| return MemorySessionError::InternalError(e.to_string()))
            .ok();
        cache
            .add_turn("Value 2".to_string())
            .map_err(|e| MemorySessionError::InternalError(e.to_string()))
            .ok();
        assert_eq!(cache.get_size(), 2);
    }
}
