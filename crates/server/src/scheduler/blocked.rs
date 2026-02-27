use std::collections::HashSet;

use crate::{
    data_model::SandboxKey,
    scheduler::executor_class::ExecutorClass,
    state_store::in_memory_state::FunctionRunKey,
};

/// Information about why a workload was blocked.
#[derive(Debug, Clone)]
pub struct BlockingInfo {
    /// Executor classes that passed constraint checks but lacked resources.
    pub eligible_classes: HashSet<ExecutorClass>,
    /// If true, the workload should always be retried on any capacity change
    /// (e.g., when no executor classes were evaluated).
    pub escaped: bool,
    /// Memory requirement in MB.
    pub memory_mb: u64,
}

/// Work that was unblocked by a capacity change.
#[derive(Debug, Default)]
pub struct UnblockedWork {
    pub sandbox_keys: Vec<SandboxKey>,
    pub function_run_keys: Vec<FunctionRunKey>,
}

impl UnblockedWork {
    pub fn is_empty(&self) -> bool {
        self.sandbox_keys.is_empty() && self.function_run_keys.is_empty()
    }
}

/// Tracks failed placements by executor class eligibility.
///
/// On capacity changes (new executor, freed resources), returns only work that
/// could benefit from the change. Mirrors Nomad's `BlockedEvals`.
///
/// Work is categorized as:
/// - **Class-blocked**: has eligible classes — retry when capacity freed for
///   that class.
/// - **Escaped**: no classes evaluated (e.g., zero executors at time of
///   placement) — always retry on any capacity change.
#[derive(Debug, Clone, Default)]
pub struct BlockedWorkTracker {
    /// Sandboxes that failed placement, keyed by sandbox key.
    blocked_sandboxes: imbl::HashMap<SandboxKey, BlockingInfo>,
    /// Function runs that failed placement, keyed by function run key.
    blocked_function_runs: imbl::HashMap<FunctionRunKey, BlockingInfo>,

    // Reverse indexes for O(1) unblocking
    /// Sandbox keys blocked on a specific executor class.
    sandboxes_by_class: imbl::HashMap<ExecutorClass, imbl::HashSet<SandboxKey>>,
    /// Function run keys blocked on a specific executor class.
    function_runs_by_class: imbl::HashMap<ExecutorClass, imbl::HashSet<FunctionRunKey>>,

    /// Escaped sandboxes — always retry on any capacity change.
    escaped_sandboxes: imbl::HashSet<SandboxKey>,
    /// Escaped function runs — always retry on any capacity change.
    escaped_function_runs: imbl::HashSet<FunctionRunKey>,
}

impl BlockedWorkTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a failed sandbox placement.
    pub fn block_sandbox(&mut self, key: SandboxKey, info: BlockingInfo) {
        if info.escaped {
            self.escaped_sandboxes.insert(key.clone());
        }
        for class in &info.eligible_classes {
            self.sandboxes_by_class
                .entry(class.clone())
                .or_default()
                .insert(key.clone());
        }
        self.blocked_sandboxes.insert(key, info);
    }

    /// Record a failed function run placement.
    pub fn block_function_run(&mut self, key: FunctionRunKey, info: BlockingInfo) {
        if info.escaped {
            self.escaped_function_runs.insert(key.clone());
        }
        for class in &info.eligible_classes {
            self.function_runs_by_class
                .entry(class.clone())
                .or_default()
                .insert(key.clone());
        }
        self.blocked_function_runs.insert(key, info);
    }

    /// Unblock work eligible for a given executor class.
    ///
    /// Called when a new executor joins or an executor's class changes.
    /// Returns all escaped work + work blocked on the given class.
    pub fn unblock_for_class(&mut self, class: &ExecutorClass) -> UnblockedWork {
        let mut work = UnblockedWork::default();

        // Always include escaped work — collect first to avoid borrow conflict
        let escaped_sandboxes: Vec<_> = self.escaped_sandboxes.iter().cloned().collect();
        self.escaped_sandboxes.clear();
        for key in escaped_sandboxes {
            self.remove_sandbox_from_indexes(&key);
            self.blocked_sandboxes.remove(&key);
            work.sandbox_keys.push(key);
        }
        let escaped_fns: Vec<_> = self.escaped_function_runs.iter().cloned().collect();
        self.escaped_function_runs.clear();
        for key in escaped_fns {
            self.remove_function_run_from_indexes(&key);
            self.blocked_function_runs.remove(&key);
            work.function_run_keys.push(key);
        }

        // Unblock sandboxes eligible for this class
        if let Some(sandbox_keys) = self.sandboxes_by_class.remove(class) {
            for key in sandbox_keys {
                self.remove_sandbox_from_indexes(&key);
                self.blocked_sandboxes.remove(&key);
                work.sandbox_keys.push(key);
            }
        }

        // Unblock function runs eligible for this class
        if let Some(function_run_keys) = self.function_runs_by_class.remove(class) {
            for key in function_run_keys {
                self.remove_function_run_from_indexes(&key);
                self.blocked_function_runs.remove(&key);
                work.function_run_keys.push(key);
            }
        }

        work
    }

    /// Unblock work that could fit in freed resources for a given class.
    ///
    /// Called when a container terminates and resources are freed on an
    /// executor of the given class. Only unblocks work whose memory
    /// requirement fits within the freed capacity.
    pub fn unblock_for_freed_resources(
        &mut self,
        class: &ExecutorClass,
        freed_mb: u64,
    ) -> UnblockedWork {
        let mut work = UnblockedWork::default();

        // Always include escaped work
        let escaped_sandboxes: Vec<_> = self.escaped_sandboxes.iter().cloned().collect();
        self.escaped_sandboxes.clear();
        for key in escaped_sandboxes {
            self.remove_sandbox_from_indexes(&key);
            self.blocked_sandboxes.remove(&key);
            work.sandbox_keys.push(key);
        }
        let escaped_fns: Vec<_> = self.escaped_function_runs.iter().cloned().collect();
        self.escaped_function_runs.clear();
        for key in escaped_fns {
            self.remove_function_run_from_indexes(&key);
            self.blocked_function_runs.remove(&key);
            work.function_run_keys.push(key);
        }

        // Unblock sandboxes for this class that fit within freed resources
        if let Some(sandbox_keys) = self.sandboxes_by_class.get(class).cloned() {
            let mut to_remove = Vec::new();
            for key in &sandbox_keys {
                if let Some(info) = self.blocked_sandboxes.get(key) &&
                    info.memory_mb <= freed_mb
                {
                    to_remove.push(key.clone());
                    work.sandbox_keys.push(key.clone());
                }
            }
            for key in &to_remove {
                self.remove_sandbox_from_indexes(key);
                self.blocked_sandboxes.remove(key);
            }
        }

        // Unblock function runs for this class that fit within freed resources
        if let Some(fn_keys) = self.function_runs_by_class.get(class).cloned() {
            let mut to_remove = Vec::new();
            for key in &fn_keys {
                if let Some(info) = self.blocked_function_runs.get(key) &&
                    info.memory_mb <= freed_mb
                {
                    to_remove.push(key.clone());
                    work.function_run_keys.push(key.clone());
                }
            }
            for key in &to_remove {
                self.remove_function_run_from_indexes(key);
                self.blocked_function_runs.remove(key);
            }
        }

        work
    }

    /// Remove a sandbox from the tracker (e.g., when it completes or is
    /// terminated).
    pub fn remove_sandbox(&mut self, key: &SandboxKey) {
        self.remove_sandbox_from_indexes(key);
        self.blocked_sandboxes.remove(key);
        self.escaped_sandboxes.remove(key);
    }

    /// Remove a function run from the tracker (e.g., when it completes or
    /// fails).
    pub fn remove_function_run(&mut self, key: &FunctionRunKey) {
        self.remove_function_run_from_indexes(key);
        self.blocked_function_runs.remove(key);
        self.escaped_function_runs.remove(key);
    }

    /// Returns true if there is no blocked work at all.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.blocked_sandboxes.is_empty() && self.blocked_function_runs.is_empty()
    }

    /// Number of blocked sandboxes.
    #[cfg(test)]
    pub fn blocked_sandbox_count(&self) -> usize {
        self.blocked_sandboxes.len()
    }

    /// Number of blocked function runs.
    #[cfg(test)]
    pub fn blocked_function_run_count(&self) -> usize {
        self.blocked_function_runs.len()
    }

    // --- Internal helpers ---

    fn remove_sandbox_from_indexes(&mut self, key: &SandboxKey) {
        if let Some(info) = self.blocked_sandboxes.get(key) {
            for class in &info.eligible_classes {
                if let Some(set) = self.sandboxes_by_class.get_mut(class) {
                    set.remove(key);
                    if set.is_empty() {
                        self.sandboxes_by_class.remove(class);
                    }
                }
            }
        }
    }

    fn remove_function_run_from_indexes(&mut self, key: &FunctionRunKey) {
        if let Some(info) = self.blocked_function_runs.get(key) {
            for class in &info.eligible_classes {
                if let Some(set) = self.function_runs_by_class.get_mut(class) {
                    set.remove(key);
                    if set.is_empty() {
                        self.function_runs_by_class.remove(class);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_class(label_key: &str, label_val: &str) -> ExecutorClass {
        let mut labels = std::collections::BTreeMap::new();
        labels.insert(label_key.to_string(), label_val.to_string());
        ExecutorClass {
            labels,
            allowlist: None,
        }
    }

    #[test]
    fn test_unblock_for_class_returns_eligible_work() {
        let mut tracker = BlockedWorkTracker::new();
        let class_a = make_class("gpu", "true");
        let class_b = make_class("gpu", "false");

        let key1 = SandboxKey::new("ns", "sandbox-1");
        let key2 = SandboxKey::new("ns", "sandbox-2");

        // sandbox-1 blocked on class_a
        tracker.block_sandbox(
            key1.clone(),
            BlockingInfo {
                eligible_classes: HashSet::from([class_a.clone()]),

                escaped: false,
                memory_mb: 512,
            },
        );

        // sandbox-2 blocked on class_b
        tracker.block_sandbox(
            key2.clone(),
            BlockingInfo {
                eligible_classes: HashSet::from([class_b.clone()]),

                escaped: false,
                memory_mb: 256,
            },
        );

        // Unblock for class_a — only sandbox-1 should be returned
        let unblocked = tracker.unblock_for_class(&class_a);
        assert_eq!(unblocked.sandbox_keys.len(), 1);
        assert!(unblocked.sandbox_keys.contains(&key1));
        assert!(unblocked.function_run_keys.is_empty());

        // sandbox-2 should still be blocked
        assert_eq!(tracker.blocked_sandbox_count(), 1);
    }

    #[test]
    fn test_escaped_work_always_unblocked() {
        let mut tracker = BlockedWorkTracker::new();
        let class_a = make_class("gpu", "true");

        let key1 = SandboxKey::new("ns", "sandbox-escaped");

        tracker.block_sandbox(
            key1.clone(),
            BlockingInfo {
                eligible_classes: HashSet::new(),

                escaped: true,
                memory_mb: 512,
            },
        );

        // Unblock for any class — escaped work should always be returned
        let unblocked = tracker.unblock_for_class(&class_a);
        assert_eq!(unblocked.sandbox_keys.len(), 1);
        assert!(unblocked.sandbox_keys.contains(&key1));
    }

    #[test]
    fn test_unblock_for_freed_resources_filters_by_memory() {
        let mut tracker = BlockedWorkTracker::new();
        let class_a = make_class("gpu", "true");

        let key_small = SandboxKey::new("ns", "sandbox-small");
        let key_large = SandboxKey::new("ns", "sandbox-large");

        tracker.block_sandbox(
            key_small.clone(),
            BlockingInfo {
                eligible_classes: HashSet::from([class_a.clone()]),

                escaped: false,
                memory_mb: 256,
            },
        );

        tracker.block_sandbox(
            key_large.clone(),
            BlockingInfo {
                eligible_classes: HashSet::from([class_a.clone()]),

                escaped: false,
                memory_mb: 2048,
            },
        );

        // Free 512 MB — only the small sandbox should be unblocked
        let unblocked = tracker.unblock_for_freed_resources(&class_a, 512);
        assert_eq!(unblocked.sandbox_keys.len(), 1);
        assert!(unblocked.sandbox_keys.contains(&key_small));

        // Large sandbox still blocked
        assert_eq!(tracker.blocked_sandbox_count(), 1);
    }

    #[test]
    fn test_remove_sandbox_cleans_up() {
        let mut tracker = BlockedWorkTracker::new();
        let class_a = make_class("gpu", "true");

        let key1 = SandboxKey::new("ns", "sandbox-1");
        tracker.block_sandbox(
            key1.clone(),
            BlockingInfo {
                eligible_classes: HashSet::from([class_a.clone()]),

                escaped: false,
                memory_mb: 512,
            },
        );

        assert_eq!(tracker.blocked_sandbox_count(), 1);
        tracker.remove_sandbox(&key1);
        assert_eq!(tracker.blocked_sandbox_count(), 0);
        assert!(tracker.is_empty());
    }

    #[test]
    fn test_function_run_blocking() {
        let mut tracker = BlockedWorkTracker::new();
        let class_a = make_class("region", "us-east");

        let key1 = FunctionRunKey::new("ns|app|req|fn-1");

        tracker.block_function_run(
            key1.clone(),
            BlockingInfo {
                eligible_classes: HashSet::from([class_a.clone()]),

                escaped: false,
                memory_mb: 1024,
            },
        );

        assert_eq!(tracker.blocked_function_run_count(), 1);

        let unblocked = tracker.unblock_for_class(&class_a);
        assert_eq!(unblocked.function_run_keys.len(), 1);
        assert!(unblocked.function_run_keys.contains(&key1));
        assert!(tracker.is_empty());
    }
}
