mod config;
mod mqtt;

pub use config::*;
pub use mqtt::*;

pub enum Availability {
    FirstRun,
    True,
    False,
}

impl PartialEq<bool> for Availability {
    fn eq(&self, other: &bool) -> bool {
        match self {
            Availability::FirstRun => false,
            Availability::True => *other,
            Availability::False => !*other,
        }
    }
}

impl From<bool> for Availability {
    fn from(value: bool) -> Self {
        if value {
            Availability::True
        } else {
            Availability::False
        }
    }
}
