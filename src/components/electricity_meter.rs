/// A model of an electricity meter, designed to display it via MQTT. This generates discovery
/// requests and the corresponding status messages. It also tracks four sensor values: energy
/// input, energy output, instantaneous power and average power.
use super::{InfluxDb, PhysicalQuantity};

use crate::{
    utils::*,
    utils::{MqttMessage, MqttMessages},
};
use log::*;
use serde::Serialize;

/// The period over which the average is calculated
pub const EMETER_MEAN_TIME: i32 = 60;

/// EValue is intended for passing sensor values.
#[allow(unused)]
pub enum EValue {
    EnergyInput {
        e_in: f64,
    },
    EnergyInputIncrement {
        e_in_inc: f64,
    },
    EnergyOutput {
        e_out: f64,
    },
    EnergyOutputIncrement {
        e_out_inc: f64,
    },
    SecPower {
        sec_power: f64,
    },
    Power {
        power: f64,
    },
    All {
        sec_power: f64,
        e_in: f64,
        e_out: f64,
    },
    Nothing,
}

/// Details for the discovery message
#[derive(Debug, Serialize)]
pub struct ConstEMeter {
    pub e_in: &'static TopicAndSensor,
    pub e_out: &'static TopicAndSensor,
    pub power: &'static TopicAndSensor,
    pub sec_power: &'static TopicAndSensor,
}

/// The electricity meter structure
pub struct EMeter {
    e_in: f64,
    e_out: f64,
    sec_power: f64,
    power: f64,
    sum_power: f64,
    avail: Availability,
    avail_e_in: bool,
    avail_e_out: bool,
    tick_count: i32,

    influxdb: InfluxDb,
    config: &'static ConstEMeter,
}

impl EMeter {
    /// Create a EMeter struct
    pub fn new(
        influxdb: &InfluxDb,
        config: &'static ConstEMeter,
    ) -> Self {
        EMeter {
            e_in: 0.0,
            e_out: 0.0,
            sec_power: 0.0,
            power: 0.0,
            sum_power: 0.0,
            avail: Availability::FirstRun,
            avail_e_in: false,
            avail_e_out: false,
            tick_count: 0,

            influxdb: influxdb.clone(),
            config,
        }
    }

    /// Create the discovery messages for the electricity meter
    pub async fn power_up_msgs(&mut self) -> MqttMessages {
        let mut msgs = MqttMessages::new();

        let payload = serde_json::to_string(self.config.e_in.sensor).unwrap();
        msgs += MqttMessage::new(self.config.e_in.topic, payload)
            .set_qos(rumqttc::QoS::AtLeastOnce)
            .set_retain(true);

        let payload = serde_json::to_string(self.config.e_out.sensor).unwrap();
        msgs += MqttMessage::new(self.config.e_out.topic, payload)
            .set_qos(rumqttc::QoS::AtLeastOnce)
            .set_retain(true);

        let payload = serde_json::to_string(self.config.power.sensor).unwrap();
        msgs += MqttMessage::new(self.config.power.topic, payload)
            .set_qos(rumqttc::QoS::AtLeastOnce)
            .set_retain(true);

        let payload = serde_json::to_string(self.config.sec_power.sensor).unwrap();
        msgs += MqttMessage::new(self.config.sec_power.topic, payload)
            .set_qos(rumqttc::QoS::AtLeastOnce)
            .set_retain(true);

        let mut task = EmeterTask::default();
        if let Ok(energy) = self
            .influxdb
            .get_value(
                self.config.e_in.sensor.unique_id,
                PhysicalQuantity::Energy,
            )
            .await
        {
            trace!(
                "Read from InfluxDb {}: {:.0} {}",
                self.config.e_in.sensor.unique_id,
                energy,
                PhysicalQuantity::Energy.unit()
            );
            self.e_in = energy;
            self.avail_e_in = true;
            task.e_in = true;
        }

        if let Ok(energy) = self
            .influxdb
            .get_value(
                self.config.e_out.sensor.unique_id,
                PhysicalQuantity::Energy,
            )
            .await
        {
            trace!(
                "Read from InfluxDb {}: {:.0} {}",
                self.config.e_out.sensor.unique_id,
                energy,
                PhysicalQuantity::Energy.unit()
            );
            self.e_out = energy;
            self.avail_e_out = true;
            task.e_out = true;
        }

        if let Ok(power) = self
            .influxdb
            .get_value(
                self.config.power.sensor.unique_id,
                PhysicalQuantity::Power,
            )
            .await
        {
            trace!(
                "Read from InfluxDb {}: {:.0} {}",
                self.config.power.sensor.unique_id,
                power,
                PhysicalQuantity::Power.unit()
            );
            self.power = power;
            task.power = true;
        }
        msgs += self.create_message(task, false).await;
        msgs
    }

    /// Read instantaneous power if available
    #[allow(unused)]
    pub fn get_sec_power(&self) -> Option<f64> {
        if self.avail == true {
            return Some(self.sec_power);
        }
        None
    }

    /// Read average power if available
    #[allow(unused)]
    pub fn get_power(&self) -> Option<f64> {
        if self.avail == true {
            return Some(self.power);
        }
        None
    }

    /// Returns the mean value of the performance if it has just been calculated
    #[allow(unused)]
    pub fn get_new_power(&self) -> Option<f64> {
        if self.avail == true && self.tick_count == 0 {
            return Some(self.power);
        }
        None
    }

    /// Read Energy consumption if available
    #[allow(unused)]
    pub fn get_e_in(&self) -> Option<f64> {
        if self.avail_e_in {
            return Some(self.e_in);
        }
        None
    }

    /// Read energy production if available
    #[allow(unused)]
    pub fn get_e_out(&self) -> Option<f64> {
        if self.avail_e_in {
            return Some(self.e_out);
        }
        None
    }

    /// Call this routine every second to maintain connection and average values
    pub async fn tick_1hz(&mut self, avail: bool) -> MqttMessages {
        // prepare result vec
        let mut msgs = MqttMessages::new();

        // check changes in availability
        if self.avail != avail {
            // set availability topic to online/offline
            let payload = if avail { "online" } else { "offline" };
            msgs += MqttMessage::new(self.config.e_in.sensor.availability_topic, payload)
                .set_retain(true);

            // if avail == true send all possible sensor values
            // after being offline we are now first time online
            if avail {
                // set power to sec_power
                self.power = self.sec_power;
                self.tick_count = 0;
                self.sum_power = 0.0;

                // create mqtt message for all known sensor values
                let mut task = EmeterTask::activate_power();
                task.e_in = self.avail_e_in;
                task.e_out = self.avail_e_out;
                msgs += self.create_message(task, false).await
            }
            self.avail = Availability::from(avail);
        }

        if self.avail == true {
            self.tick_count += 1;
            self.sum_power += self.sec_power;

            // calc 1 min mean power
            if self.tick_count >= EMETER_MEAN_TIME {
                self.power = self.sum_power / self.tick_count as f64;
                self.tick_count = 0;
                self.sum_power = 0.0;
                let task = EmeterTask::activate_power();
                msgs += self.create_message(task, true).await
            }
        }
        msgs
    }

    /// Set sensor values
    pub async fn set_value(&mut self, value: EValue) -> MqttMessages {
        let mut task = EmeterTask::default();
        match value {
            EValue::All {
                sec_power,
                e_in,
                e_out,
            } => {
                if sec_power != self.sec_power || self.tick_count == 0 {
                    self.sec_power = sec_power;
                    task.sec_power = true;
                }
                if e_in != self.e_in {
                    self.e_in = e_in;
                    task.e_in = true;
                }
                if e_out != self.e_out {
                    self.e_out = e_out;
                    task.e_out = true;
                }
            }
            EValue::EnergyInput { e_in } => {
                if e_in != self.e_in {
                    self.e_in = e_in;
                    task.e_in = true;
                }
            }
            EValue::EnergyInputIncrement { e_in_inc } => {
                if e_in_inc != 0.0 {
                    self.e_in += e_in_inc;
                    task.e_in = true;
                }
            }
            EValue::EnergyOutput { e_out } => {
                if e_out != self.e_out {
                    self.e_out = e_out;
                    task.e_out = true;
                }
            }
            EValue::EnergyOutputIncrement { e_out_inc } => {
                if e_out_inc != 0.0 {
                    self.e_out += e_out_inc;
                    task.e_out = true;
                }
            }
            EValue::SecPower { sec_power } => {
                if sec_power != self.sec_power || self.tick_count == 0 {
                    self.sec_power = sec_power;
                    task.sec_power = true;
                }
            }
            EValue::Power { power } => {
                if power != self.power {
                    self.power = power;
                    task.power = true;
                }
            }
            EValue::Nothing => (),
        }

        MqttMessages::from_msg(self.create_message(task, true).await)
    }

    async fn create_message(&mut self, task: EmeterTask, write_to_db: bool) -> MqttMessage {
        if task.e_in {
            if write_to_db {
                let sensor_id = self.config.e_in.sensor.unique_id;
                let quantity = PhysicalQuantity::Energy;
                let value = self.e_in;
                let _ = self.influxdb.set_value(sensor_id, quantity, value).await;
                trace!(
                    "Write to InfluxDb {}: {:.0} {}",
                    sensor_id,
                    value,
                    quantity.unit()
                );
            }
        }
        if task.e_out {
            if write_to_db {
                let sensor_id = self.config.e_out.sensor.unique_id;
                let quantity = PhysicalQuantity::Energy;
                let value = self.e_out;
                let _ = self.influxdb.set_value(sensor_id, quantity, value).await;
                trace!(
                    "Write to InfluxDb {}: {:.0} {}",
                    sensor_id,
                    value,
                    quantity.unit()
                );
            }
        }
        if task.power {
            if write_to_db {
                let sensor_id = self.config.power.sensor.unique_id;
                let quantity = PhysicalQuantity::Power;
                let value = self.power;
                let _ = self.influxdb.set_value(sensor_id, quantity, value).await;
                trace!(
                    "Write to InfluxDb {}: {:.0} {}",
                    sensor_id,
                    value,
                    quantity.unit()
                );
            }
        }
        let msg = format!(
            r#"{{"e_in": {:.3}, "e_out": {:.3}, "sec_power": {:.3}, "power": {:.3}}}"#,
            self.e_in, self.e_out, self.sec_power, self.power,
        );
        MqttMessage::new(self.config.e_in.sensor.state_topic, msg)
    }
}

#[derive(Default)]
pub struct EmeterTask {
    pub e_in: bool,
    pub e_out: bool,
    pub power: bool,
    pub sec_power: bool,
}

impl EmeterTask {
    pub fn activate_power() -> Self {
        EmeterTask {
            power: true,
            ..Default::default()
        }
    }
}
