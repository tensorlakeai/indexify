use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn timestamp_secs() -> u64 {
    let now = SystemTime::now();
    let duration = now.duration_since(UNIX_EPOCH).unwrap();
    duration.as_secs()
}

#[derive(Debug, Clone)]
pub struct PostgresIndexName(String);

impl PostgresIndexName {
    pub fn new(index_name: &str) -> PostgresIndexName {
        let name = index_name.replace('-', "_");
        let name = name.replace('.', "_");
        Self(name)
    }
}

impl fmt::Display for PostgresIndexName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

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
