pub mod generic_health_checker;

/// Abstract trait for health checkers.
pub trait HealthChecker {
    /// Handle changes in server connection state.
    fn server_connection_state_changed(&mut self, is_healthy: bool, status_message: String);

    fn check(&self) -> Self;
}
