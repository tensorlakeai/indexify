use indexify_internal_api::StateChange;

use crate::state::SharedState;

pub struct Scheduler {
    shared_state: SharedState,
}

impl Scheduler {
    pub fn new(shared_state: SharedState) -> Self {
        Scheduler { shared_state }
    }

    pub async fn handle_change_event(&self, change_events: StateChange) {}
}
