use std::sync::Arc;

use crate::executor::{metrics::Metrics, monitoring::health_checker::HealthChecker};

pub const HEALTH_CHECKER_NAME: &str = "GenericHealthChecker";

#[derive(Debug, Clone)]
pub struct GenericHealthChecker {
    pub is_success: bool,
    pub status_message: Option<String>,
    pub checker_name: String,
    pub metrics: Arc<Metrics>,
}

impl GenericHealthChecker {
    pub fn new(
        is_success: bool,
        status_message: Option<String>,
        checker_name: String,
        metrics: Arc<Metrics>,
    ) -> Self {
        metrics.healthy_gauge.set(1);
        GenericHealthChecker {
            is_success,
            status_message,
            checker_name,
            metrics,
        }
    }
}

impl HealthChecker for GenericHealthChecker {
    fn server_connection_state_changed(&mut self, is_healthy: bool, status_message: String) {
        if is_healthy {
            self.status_message = None;
            self.is_success = true;
            self.checker_name = HEALTH_CHECKER_NAME.to_string();
            self.metrics.healthy_gauge.set(1);
        } else {
            self.status_message = Some(status_message);
            self.is_success = false;
            self.checker_name = HEALTH_CHECKER_NAME.to_string();
            self.metrics.healthy_gauge.set(0);
        }
    }

    fn check(&self) -> Self {
        self.clone()
    }
}
