mod config;
mod mqtt;

pub use config::*;
pub use mqtt::*;

use serde::Serialize;

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

/// Details for the discovery message
#[derive(Debug, Serialize)]
pub struct Sensor2 {
    pub name: &'static str,
    pub platform: &'static str,
    pub unique_id: &'static str,
    pub enabled_by_default: bool,
    pub state_topic: &'static str,
    pub availability_topic: &'static str,
    pub unit_of_measurement: &'static str,
    pub device_class: &'static str,
    pub state_class: &'static str,
    pub value_template: &'static str,
    pub suggested_display_precision: u8,
}
