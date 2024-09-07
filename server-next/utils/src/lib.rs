use std::time::{SystemTime, UNIX_EPOCH};

#[macro_export]
macro_rules! unwrap_or_continue {
    ($opt: expr) => {
        match $opt {
            Some(v) => v,
            None => {
                continue;
            }
        }
    };
}

pub trait OptionInspectNone<T> {
    fn inspect_none(self, inspector_function: impl FnOnce()) -> Self;
}

impl<T> OptionInspectNone<T> for Option<T> {
    fn inspect_none(self, inspector_function: impl FnOnce()) -> Self {
        match &self {
            Some(_) => (),
            None => inspector_function(),
        }
        self
    }
}

impl<T> OptionInspectNone<T> for &Option<T> {
    fn inspect_none(self, inspector_function: impl FnOnce()) -> Self {
        match &self {
            Some(_) => (),
            None => inspector_function(),
        }
        self
    }
}

pub fn get_epoch_time_in_ms() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH");
    since_the_epoch.as_millis() as u64
}

pub fn default_creation_time() -> SystemTime {
    UNIX_EPOCH
}
