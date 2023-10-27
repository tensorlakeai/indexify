use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{internal_api::Work, internal_api::WorkStatus};

pub struct WorkStore {
    allocated_work: Arc<RwLock<HashMap<String, Work>>>,
    completed_work: Arc<RwLock<HashMap<String, WorkStatus>>>,
}

impl WorkStore {
    pub fn new() -> Self {
        Self {
            allocated_work: Arc::new(RwLock::new(HashMap::new())),
            completed_work: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn clear_completed_work(&self) {
        self.completed_work.write().unwrap().clear();
    }

    pub fn add_work_list(&self, work_list: Vec<Work>) {
        let mut allocated_work = self.allocated_work.write().unwrap();
        for work in work_list {
            allocated_work.insert(work.id.clone(), work);
        }
    }

    pub fn update_work_status(&self, work_status: Vec<WorkStatus>) {
        let mut allocated_work_handle = self.allocated_work.write().unwrap();
        let mut completed_work_handle = self.completed_work.write().unwrap();
        for work in work_status {
            allocated_work_handle.remove(&work.work_id);
            completed_work_handle.insert(work.work_id.clone(), work);
        }
    }

    pub fn pending_work(&self) -> Vec<Work> {
        let allocated_work = self.allocated_work.read().unwrap();
        allocated_work.values().cloned().collect()
    }

    pub fn completed_work(&self) -> Vec<WorkStatus> {
        let allocated_work = self.completed_work.read().unwrap();
        allocated_work.values().cloned().collect()
    }
}
