use super::{InfluxDb, PhysicalQuantity};
use crate::utils::*;
use crate::utils::{MqttMessage, MqttMessages};
use log::*;

const MAX_TICK: u32 = 60;

pub struct Control {
    control_value: f64,
    tick: u32,

    influxdb: InfluxDb,
    config: &'static SensorConfig,
}

impl Control {
    pub fn new(
        influxdb: &InfluxDb,
        config: &'static SensorConfig,
    ) -> Self {
        Control {
            control_value: 0.0,
            tick: 0,
            influxdb: influxdb.clone(),
            config,
        }
    }

    pub async fn power_up_msgs(&mut self) -> MqttMessages {
        let payload = serde_json::to_string(self.config).unwrap();
        let msg = MqttMessage::new(self.config.topic, payload)
            .set_qos(rumqttc::QoS::AtLeastOnce)
            .set_retain(true);
        let mut msgs = MqttMessages::from_msg(msg);

        if let Ok(cv) = self
            .influxdb
            .get_value(
                self.config.payload.unique_id,
                PhysicalQuantity::Power,
            )
            .await
        {
            trace!(
                "Read from InfluxDb {}: {:.0} {}",
                self.config.payload.unique_id,
                cv,
                PhysicalQuantity::Power.unit()
            );
            msgs += self.set_cv(cv, false).await;
        }

        msgs
    }

    #[allow(unused)]
    pub fn get_control_value(&self) -> f64 {
        self.control_value
    }

    pub async fn set_control_value(&mut self, cv: f64) -> MqttMessages {
        self.tick = (self.tick + 1) % MAX_TICK;
        MqttMessages::from_msg(self.set_cv(cv, self.tick == 0).await)
    }

    async fn set_cv(&mut self, cv: f64, write_to_db: bool) -> MqttMessage {
        self.control_value = cv;

        let sensor_id = self.config.payload.unique_id;
        let quantity = PhysicalQuantity::Power;

        if write_to_db {
            let _ = self.influxdb.set_value(sensor_id, quantity, cv).await;
            trace!(
                "Write to InfluxDb {}: {:.0} {}",
                sensor_id,
                cv,
                quantity.unit()
            );
        }

        MqttMessage::new(
            self.config.payload.state_topic,
            format!(r#"{{"control_value": {}}}"#, self.control_value.round()),
        )
    }
}
