use super::{InfluxDb, PhysicalQuantity};
use crate::utils::*;
use crate::utils::{MqttMessage, MqttMessages};
use serde_json::Value;
use chrono::{Datelike, NaiveTime};
use log::*;

const MAX_TICK: u32 = 60;

#[derive(PartialEq, Debug)]
pub enum BatteryState {
    IsFull,
    EnoughForCar,
    NotEnoughForCar,
    IsEmpty,
}

#[derive(PartialEq, Debug)]
pub enum Target {
    ChargeLevel85,
    ChargeLevel100PeekThan85,
    ChargeLevel100,
}

pub struct Battery {
    soc: f64,
    tick: u32,
    was_full: bool,
    was_enough: bool,
    target: Target,
    room_temperature: f64,

    influxdb: InfluxDb,
    config: &'static SensorConfig,
}

impl Battery {
    pub fn new(
        influxdb: &InfluxDb,
        config: &'static SensorConfig,
    ) -> Self {
        Battery {
            soc: 0.0,
            tick: 0,
            was_full: false,
            was_enough: false,
            target: Target::ChargeLevel85,
            room_temperature: 20.0,
            influxdb: influxdb.clone(),
            config,
        }
    }

    pub async fn power_up_msgs(&mut self) -> MqttMessages {
        let payload = serde_json::to_string(self.config.payload).unwrap();
        let msg = MqttMessage::new(self.config.topic, payload)
            .set_qos(rumqttc::QoS::AtLeastOnce)
            .set_retain(true);
        let mut msgs = MqttMessages::from_msg(msg);

        if let Ok(soc) = self
            .influxdb
            .get_value(
                self.config.payload.unique_id,
                PhysicalQuantity::Battery,
            )
            .await
        {
            trace!(
                "Read from InfluxDb {}: {:.0} {}",
                self.config.payload.unique_id,
                soc,
                PhysicalQuantity::Battery.unit()
            );
            msgs += self.set_soc(soc, false).await;
        }

        msgs
    }

    #[allow(unused)]
    pub fn get_state_of_charge(&self) -> f64 {
        self.soc
    }

    pub async fn set_state_of_charge(&mut self, soc: f64) -> MqttMessages {
        self.tick = (self.tick + 1) % MAX_TICK;
        if soc != self.soc || self.tick == 0 {
            MqttMessages::from_msg(self.set_soc(soc, true).await)
        } else {
            MqttMessages::new()
        }
    }

    #[allow(unused)]
    pub fn tick_1hz(&mut self) {
        let now = chrono::Local::now();
        let start_time = NaiveTime::from_hms_opt(4, 0, 0).unwrap();
        let end_time = NaiveTime::from_hms_opt(4, 0, 3).unwrap();
        if now.time() >= start_time && now.time() <= end_time {
            self.target = if now.weekday() == chrono::Weekday::Mon || 
                            self.target == Target::ChargeLevel100PeekThan85
                { Target::ChargeLevel100PeekThan85 } 
            else {
                Target::ChargeLevel85 
            };
            if self.room_temperature < 22.5 {
                self.target = Target::ChargeLevel100;
            }
            trace!("Set new battery target {:?}", self.target);
        } 
    }

    pub async fn set_room_temp(&mut self, payload: &[u8]) -> MqttMessages {
        if let Ok(number) = serde_json::from_slice::<Value>(payload) {
            if let Some(temperature) = number.as_f64() {
                self.room_temperature = temperature;
                trace!("Room temperature {}", self.room_temperature);
            }
        }
        MqttMessages::new()
    }

    #[allow(unused)]
    pub fn is_full(&mut self) -> bool {
        let is_full = match self.target {
            Target::ChargeLevel100PeekThan85 => {
                if self.soc > 99.5 {
                    self.target = Target::ChargeLevel85;
                    true
                } else {
                    false
                }
            },
            Target::ChargeLevel85 => {
                if self.was_full {  // 1% hysteresis
                    self.soc > 84.5
                } else {
                    self.soc > 85.5
                }            

            },
            Target::ChargeLevel100 => self.soc > 99.5,
        };
        self.was_full = is_full;
        is_full
    }

    pub fn enough_for_car(&mut self) -> bool {
        let limit = match self.target {
            Target::ChargeLevel100 => 64.5,
            Target::ChargeLevel100PeekThan85 | Target::ChargeLevel85 => 49.5,
        };
        let is_enough = if self.was_enough {
            self.soc > limit
        } else {
            self.soc > limit + 1.0
        };
        self.was_enough = is_enough;
        is_enough 
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.soc < 5.1
    }
    
    pub fn state(&mut self) -> BatteryState {
        if self.is_full() {
            BatteryState::IsFull
        } else if self.is_empty() {
            BatteryState::IsEmpty
        } else if self.enough_for_car() {
            BatteryState::EnoughForCar
            } else {
                BatteryState::NotEnoughForCar
            }
    }

    async fn set_soc(&mut self, soc: f64, write_to_db: bool) -> MqttMessage {
        self.soc = soc;

        let sensor_id = self.config.payload.unique_id;
        let quantity = PhysicalQuantity::Battery;

        if write_to_db {
            let _ = self.influxdb.set_value(sensor_id, quantity, soc).await;
            trace!(
                "Write to InfluxDb {}: {:.0} {}",
                sensor_id,
                soc,
                quantity.unit()
            );
        }

        MqttMessage::new(
            self.config.payload.state_topic,
            format!(r#"{{"soc": {:.0}}}"#, self.soc),
        )
    }
}
