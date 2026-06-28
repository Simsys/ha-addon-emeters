/// Simple Model of a electricity meter of e-go wallbox
use crate::components::*;
use crate::devices::*;
use crate::utils::*;
use serde_json::Value;

#[derive(Debug, Default, Clone, Copy)]
pub enum CarState {
    #[default]
    Unknown,
    Idle,
    Charging,
    WaitCar,
    Complete,
    Error,
    Intialisiing,
}

impl From<u64> for CarState {
    fn from(value: u64) -> Self {
        match value {
            1 => CarState::Idle,
            2 => CarState::Charging,
            3 => CarState::WaitCar,
            4 => CarState::Complete,
            5 => CarState::Error,
            6 => CarState::Intialisiing,
            _ => CarState::Unknown,
        }
    }
}

const MEAN_CNT: u32 = 60;

struct CarControl {
    tick_cnt: u32,
    sum_power: f64,
    set_power: f64,
    current_power: f64,
    no_solar: bool,
}

impl CarControl {
    fn new() -> Self {
        CarControl {
            tick_cnt: 0,
            sum_power: 0.0,
            set_power: 0.0,
            current_power: 0.0,
            no_solar: false,
        }
    }

    fn update(&mut self, avail_p: f64, no_solar: bool) -> MqttMessages {
        let no_solar_changed = no_solar != self.no_solar;
        self.no_solar = no_solar;

        // calc mean value of power
        self.tick_cnt += 1;
        self.sum_power += avail_p;
        if self.tick_cnt == MEAN_CNT {
            self.tick_cnt = 0;
            self.current_power = self.sum_power / MEAN_CNT as f64;
            self.sum_power = 0.0;
        }

        // calc amp, psm and frc
        let (amp, psm, frc) = Self::get_amp_psm_frc(self.current_power, no_solar);

        // calc set power
        if frc == 1 {
            self.set_power = 0.0;
        } else {
            self.set_power = amp as f64 * 230.0 * psm as f64;
        }
        
        // if no_solar or new set power value: push it to MQTT
        let mut r = MqttMessages::new();
        if no_solar_changed || self.tick_cnt == 0 {
            trace!(
                "set_power {} amp {}, psm {}, frc {}",
                self.set_power,
                amp,
                psm,
                frc,
            );
            r += MqttMessage::new("go-eCharger/083205/amp/set", amp.to_string());
            r += MqttMessage::new("go-eCharger/083205/psm/set", psm.to_string());
            r += MqttMessage::new("go-eCharger/083205/frc/set", frc.to_string());
        }
        r
    }

    fn get_set_power(&self) -> f64 {
        self.set_power
    }

    fn get_amp_psm_frc(power: f64, no_solar: bool) -> (u8, u8, u8) {
        if no_solar {
            return (16, 2, 0);
        }
        let current = power / 230.0;
        if current < 6.0 {
            return (6, 1, 1);
        }
        if current < 14.0 {
            return (current as u8, 1, 0);
        }
        if current > 32.0 {
            (16, 2, 0)
        } else {
            ((current / 2.0) as u8, 2, 0)
        }
    }
}

impl CarState {
    #[allow(unused)]
    pub fn is_connected(&self) -> bool {
        *self as u8 > 1
    }
}

pub struct Wallbox {
    tick_cnt: usize,
    eto_cnt: usize,
    no_solar: bool,
    car_state: CarState,
    connection: Connection,
    e_meter: EMeter,
    control: Control,
    car_control: CarControl,
}

impl Wallbox {
    /// Create a new meter
    pub fn new(influxdb: &InfluxDb) -> Self {
        let connection = Connection::new(WALLBOX_CONN_CONFIG_TOPIC, WALLBOX_CONN_CONFIC);
        let e_meter = EMeter::new(influxdb, WALLBOX_EMETER_CONFIG);
        let control = Control::new(
            influxdb,
            WALLBOX_CONTROL_CONFIG,
        );
        let car_control = CarControl::new();
        Wallbox {
            tick_cnt: TIMEOUT,
            eto_cnt: 0,
            no_solar: false,
            car_state: CarState::Unknown,
            connection,
            e_meter,
            control,
            car_control,
        }
    }

    pub async fn power_up_msgs(&mut self) -> MqttMessages {
        let no_solar_power_up_msg = MqttMessage::new(
            WALLBOX_NO_SOLAR_CONFIG_TOPIC,
            serde_json::to_string(WALLBOX_NO_SOLAR_CONFIG).unwrap(),
        )
        .set_qos(rumqttc::QoS::AtLeastOnce)
        .set_retain(true);

        let mut msgs = self.connection.power_up_msgs();
        msgs += no_solar_power_up_msg;
        msgs += self.e_meter.power_up_msgs().await;
        msgs += self.control.power_up_msgs().await;
        msgs
    }

    #[allow(unused)]
    pub fn get_car_state(&self) -> CarState {
        self.car_state
    }

    pub fn get_conn_state_sent(&self) -> bool {
        self.connection.state()
    }

    pub fn e_meter(&self) -> &EMeter {
        &self.e_meter
    }

    /// Has to be called once a second
    pub async fn tick_1hz(&mut self) -> MqttMessages {
        let state = if self.tick_cnt < 10 {
            self.tick_cnt += 1;
            true
        } else {
            false
        };
        let mut msgs = self.connection.set_state(state);
        msgs += self.e_meter.tick_1hz(state).await;
        msgs
    }

    pub async fn control(&mut self, avail_p: f64) -> MqttMessages {
        let mut r = self.car_control.update(avail_p, self.no_solar);
        let power = self.car_control.get_set_power();
        r += self.control.set_control_value(-power).await;
        r
    }

    pub async fn no_solar_switch(&mut self, payload: &[u8]) -> MqttMessages {
        let mut msgs = MqttMessages::new();
        match payload {
            b"on" => self.no_solar = true,
            _ => self.no_solar = false,
        }
        let msg = MqttMessage::new(
            WALLBOX_NO_SOLAR_CONFIG.state_topic, 
            str::from_utf8(payload).unwrap()
        );
        msgs += msg;
        msgs
    }

    /// Get plug state from wallbox mqtt interface
    pub async fn value_from_car(&mut self, payload: &[u8]) -> MqttMessages {
        if let Ok(number) = serde_json::from_slice::<Value>(payload) {
            if let Some(id) = number.as_u64() {
                // 0: unknown 1: idle 2: charging 3: wait car 4: complete 5: error 6: initialising
                self.car_state = CarState::from(id);
            }
        }
        trace!("CarState {:?}", self.car_state);
        MqttMessages::new()
    }

    /// Get energy value from wallbox mqtt interface
    pub async fn value_from_eto(&mut self, payload: &[u8]) -> MqttMessages {
        self.eto_cnt = (self.eto_cnt + 1) % 60;
        if self.eto_cnt != 0 {
            return MqttMessages::new();
        }
        let value = if let Ok(number) = serde_json::from_slice::<Value>(payload) {
            if let Some(energy) = number.as_f64() {
                self.tick_cnt = 0;
                EValue::EnergyInput {
                    e_in: energy / 1000.0,
                }
            } else {
                EValue::Nothing
            }
        } else {
            EValue::Nothing
        };
        self.e_meter.set_value(value).await
    }

    /// Get power value from wallbox mqtt interface
    pub async fn value_from_nrg(&mut self, payload: &[u8]) -> MqttMessages {
        let value = if let Ok(value) = serde_json::from_slice::<Value>(payload) {
            if let Some(number) = value.get(11) {
                if let Some(power) = number.as_f64() {
                    self.tick_cnt = 0;
                    EValue::SecPower { sec_power: -power }
                } else {
                    EValue::Nothing
                }
            } else {
                EValue::Nothing
            }
        } else {
            EValue::Nothing
        };
        self.e_meter.set_value(value).await
    }
}

const WALLBOX_EMETER_CONFIG: &ConstEMeter = &ConstEMeter {
    e_in: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_e_in_wallbox/config",
        payload: &Sensor {
            name: "Wallbox Energy",
            platform: "sensor",
            unique_id: "wallbox-energy-in",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/wallbox/state",
            availability_topic: "simsys/e_meter/wallbox/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_in }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    e_out: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_e_out_wallbox/config",
        payload: &Sensor {
            name: "Wallbox Energy Output",
            platform: "sensor",
            unique_id: "wallbox-energy-out",
            enabled_by_default: false,
            state_topic: "simsys/e_meter/wallbox/state",
            availability_topic: "simsys/e_meter/wallbox/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_out }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    power: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_power_wallbox/config",
        payload: &Sensor {
            name: "Wallbox Power",
            platform: "sensor",
            unique_id: "wallbox-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/wallbox/state",
            availability_topic: "simsys/e_meter/wallbox/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.power }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    sec_power: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_sec_power_wallbox/config",
        payload: &Sensor {
            name: "Wallbox Sec Power",
            platform: "sensor",
            unique_id: "wallbox-sec-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/wallbox/state",
            availability_topic: "simsys/e_meter/wallbox/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.sec_power }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
};

const WALLBOX_CONN_CONFIC: &BinarySensor = &BinarySensor {
    name: "Wallbox Connection",
    platform: "binary_sensor",
    unique_id: "connection-wallbox",
    state_topic: "simsys/connections/wallbox/state",
    device_class: "connectivity",
    device: DEVICE,
};
const WALLBOX_CONN_CONFIG_TOPIC: &str = "homeassistant/binary_sensor/simsys/connection_wallbox/config";

const WALLBOX_CONTROL_CONFIG: &SensorConfig = &SensorConfig {
    topic: "homeassistant/sensor/simsys/control_wallbox/config",
    payload: &Sensor {
        name: "Wallbox Control",
        platform: "sensor",
        unique_id: "wallbox-control",
        enabled_by_default: true,
        state_topic: "simsys/control/wallbox/state",
        availability_topic: "simsys/e_meter/wallbox/avail",
        unit_of_measurement: "W",
        device_class: "power",
        state_class: "measurement",
        value_template: "{{ value_json.control_value }}",
        suggested_display_precision: 0,
        device: DEVICE,
    }
};

const WALLBOX_NO_SOLAR_CONFIG: &Switch = &Switch {
    name: "Wallbox No Solar (always)",
    platform: "switch",
    unique_id: "wallbox-no-solar",
    command_topic: "simsys/e_meter/wallbox/no_solar_cmd",
    state_topic: "simsys/e_meter/wallbox/no_solar_state",
    payload_off: "off",
    payload_on: "on",
    device: DEVICE,
};
const WALLBOX_NO_SOLAR_CONFIG_TOPIC: &str = "homeassistant/switch/simsys/no_solar_wallbox/config";

pub fn wallbox_meter(influxdb: &InfluxDb) -> Wallbox {
    Wallbox::new(influxdb)
}
