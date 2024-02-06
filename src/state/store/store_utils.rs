use std::collections::HashMap;

use tracing::warn;

use super::{ExecutorId, ExecutorIdRef};

pub fn increment_load(
    executor_load: &mut HashMap<ExecutorId, usize>,
    executor_id: ExecutorIdRef<'_>,
) {
    let load = executor_load.entry(executor_id.to_string()).or_insert(0);
    *load += 1;
}

pub fn decrement_load(
    executor_load: &mut HashMap<ExecutorId, usize>,
    executor_id: ExecutorIdRef<'_>,
) {
    if let Some(load) = executor_load.get_mut(executor_id) {
        if *load > 0 {
            *load -= 1;
        } else {
            warn!("Tried to decrement load below 0. This is a bug because the state machine shouldn't allow it.");
        }
    } else {
        // add the executor to the load map if it's not there
        executor_load.insert(executor_id.to_string(), 0);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_increment_load() {
        let mut executor_load = HashMap::new();
        let executor_id = "executor_id";
        increment_load(&mut executor_load, &executor_id);
        assert_eq!(executor_load.get(executor_id).unwrap(), &1);
        increment_load(&mut executor_load, &executor_id);
        assert_eq!(executor_load.get(executor_id).unwrap(), &2);
    }

    #[test]
    fn test_decrement_load() {
        let mut executor_load = HashMap::new();
        let executor_id = "executor_id";
        increment_load(&mut executor_load, &executor_id);
        increment_load(&mut executor_load, &executor_id);
        decrement_load(&mut executor_load, &executor_id);
        assert_eq!(executor_load.get(executor_id).unwrap(), &1);
        decrement_load(&mut executor_load, &executor_id);
        assert_eq!(executor_load.get(executor_id).unwrap(), &0);
        decrement_load(&mut executor_load, &executor_id);
        assert_eq!(executor_load.get(executor_id).unwrap(), &0);
    }
}
