use super::{InfluxDb, PhysicalQuantity};
use crate::utils::*;
use crate::utils::{MqttMessage, MqttMessages};
use chrono::Datelike;
use log::*;

const MAX_TICK: u32 = 60;

#[derive(PartialEq, Debug)]
pub enum BatteryState {
    IsFull,
    IsEmpty,
    FullReady,
}

pub struct Battery {
    soc: f64,
    tick: u32,
    was_full: bool,

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
    pub fn is_full(&mut self) -> bool {
        let now = chrono::Local::now();
        let weekday = now.weekday();
        let is_full = match now.month() {
            // Battery care: May to September
            5|6|7|8 => match now.weekday() {
                chrono::Weekday::Mon => self.soc > 99.5,
                _ => if self.was_full {  // 1% hysteresis
                    self.soc > 84.5
                } else {
                    self.soc > 85.5
                }            
            }
            _ => self.soc > 99.5
        };
        self.was_full = is_full;
        is_full
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.soc < 5.1
    }
    
    #[allow(unused)]
    pub fn state(&mut self) -> BatteryState {
        if self.is_full() {
            BatteryState::IsFull
        } else if self.is_empty() {
            BatteryState::IsEmpty
        } else {
            BatteryState::FullReady
        }
    }

    #[allow(unused)]
    pub fn enough_for_car(&self) -> bool {
        self.soc > 49.5
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
