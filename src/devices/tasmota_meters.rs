use crate::components::*;
use crate::utils::*;
use crate::utils::{MqttMessages, TIMEOUT};
use serde::Deserialize;

/// A helper struct to handle tasmota mqtt messages
///
/// Struct to support Tasmota and Irda readers. MQTT JSON messages are processed. A 1 Hz tick
/// function helps to check for active connections.
pub struct TasmotaMeter {
    connection: Connection,
    e_meter: EMeter,
    tick_cnt: usize,
}

impl TasmotaMeter {
    /// Create a new meter
    pub fn new(
        influxdb: &InfluxDb,
        conn_topic: &'static str,
        conn_config: &'static BinarySensor,
        e_meter_config: &'static ConstEMeter,
    ) -> Self {
        let connection = Connection::new(conn_topic, conn_config);
        let e_meter = EMeter::new(influxdb, e_meter_config);
        TasmotaMeter {
            connection,
            e_meter,
            tick_cnt: TIMEOUT,
        }
    }

    pub async fn power_up_msgs(&mut self) -> MqttMessages {
        let mut msgs = self.connection.power_up_msgs();
        msgs += self.e_meter.power_up_msgs().await;
        msgs
    }

    pub fn get_conn_state_sent(&self) -> bool {
        self.connection.state()
    }

    pub fn e_meter(&self) -> &EMeter {
        &self.e_meter
    }

    /// Interprete the tasmota json strings from mqtt broker
    pub async fn value_from_json(&mut self, payload: &[u8]) -> MqttMessages {
        let value = match serde_json::from_slice::<Energy>(payload) {
            Ok(energy) => {
                // inverted: positive values is production
                let power = -energy.MT175.P;
                let e_in = energy.MT175.E_in;
                let e_out = energy.MT175.E_out;
                self.tick_cnt = 0;
                EValue::All {
                    sec_power: power,
                    e_in,
                    e_out,
                }
            }
            Err(_e) => {
                match serde_json::from_slice::<Power>(payload) {
                    Ok(power) => {
                        // inverted: positive values is production
                        let power = -power.MT175.P;
                        self.tick_cnt = 0;
                        EValue::SecPower { sec_power: power }
                    }
                    Err(_e) => {
                        //eprintln!("JSON-Fehler: {}. Rohdaten: {:?}", e, String::from_utf8_lossy(payload));
                        EValue::Nothing
                    }
                }
            }
        };
        self.e_meter.set_value(value).await
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
}

// The following stuff is to define the serde json deserialize functionality

#[allow(non_snake_case)]
#[derive(Deserialize, Debug)]
struct PowerValue {
    P: f64,
}

// {"Time":"2026-04-18T13:56:52","MT175":{"P":-9097.0}}
#[allow(non_snake_case)]
#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct Power {
    Time: String,
    MT175: PowerValue,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug)]
struct EnergyValues {
    E_in: f64,
    E_out: f64,
    P: f64,
}

// {"Time":"2026-04-18T13:55:57","MT175":{"E_in":0.0,"E_out":90012.3,"P":-5567.0}}
#[allow(non_snake_case)]
#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct Energy {
    Time: String,
    MT175: EnergyValues,
}

const Z1_EMETER_CONFIG: &ConstEMeter = &ConstEMeter {
    e_in: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_e_in_z1/config",
        payload: &Sensor {
            name: "Home Energy",
            platform: "sensor",
            unique_id: "z1-energy-in",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/z1/state",
            availability_topic: "simsys/e_meter/z1/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_in }}",
            suggested_display_precision: 0,
            device: DEVICE
        },
    },
    e_out: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_e_out_z1/config",
        payload: &Sensor {
            name: "Home Energy Output",
            platform: "sensor",
            unique_id: "z1-energy-out",
            enabled_by_default: false,
            state_topic: "simsys/e_meter/z1/state",
            availability_topic: "simsys/e_meter/z1/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_out }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    power: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_power_z1/config",
        payload: &Sensor {
            name: "Home Power",
            platform: "sensor",
            unique_id: "z1-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/z1/state",
            availability_topic: "simsys/e_meter/z1/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.power }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    sec_power: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_sec_power_z1/config",
        payload: &Sensor {
            name: "Home Sec Power",
            platform: "sensor",
            unique_id: "z1-sec-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/z1/state",
            availability_topic: "simsys/e_meter/z1/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.sec_power }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
};

const Z1_CONN_CONFIC: &BinarySensor = &BinarySensor {
    name: "Z1 Connection",
    platform: "binary_sensor",
    unique_id: "connection-z1",
    state_topic: "simsys/connections/z1/state",
    device_class: "connectivity",
    device: DEVICE,
};
const Z1_CONN_CONFIG_TOPIC: &str = "homeassistant/binary_sensor/simsys/connection_z1/config";

pub fn z1_emeter(influxdb: &InfluxDb) -> TasmotaMeter {
    TasmotaMeter::new(
        influxdb,
        Z1_CONN_CONFIG_TOPIC,
        Z1_CONN_CONFIC,
        Z1_EMETER_CONFIG,
    )
}

const Z2_EMETER_CONFIG: &ConstEMeter = &ConstEMeter {
    e_in: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_e_in_z2/config",
        payload: &Sensor {
            name: "Home Energy",
            platform: "sensor",
            unique_id: "z2-energy-in",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/z2/state",
            availability_topic: "simsys/e_meter/z2/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_in }}",
            suggested_display_precision: 0,
            device: DEVICE
        },
    },
    e_out: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_e_out_z2/config",
        payload: &Sensor {
            name: "Home Energy Output",
            platform: "sensor",
            unique_id: "z2-energy-out",
            enabled_by_default: false,
            state_topic: "simsys/e_meter/z2/state",
            availability_topic: "simsys/e_meter/z2/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_out }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    power: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_power_z2/config",
        payload: &Sensor {
            name: "Home Power",
            platform: "sensor",
            unique_id: "z2-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/z2/state",
            availability_topic: "simsys/e_meter/z2/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.power }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    sec_power: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_sec_power_z2/config",
        payload: &Sensor {
            name: "Home Sec Power",
            platform: "sensor",
            unique_id: "z2-sec-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/z2/state",
            availability_topic: "simsys/e_meter/z2/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.sec_power }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
};

const Z2_CONN_CONFIC: &BinarySensor = &BinarySensor {
    name: "Z2 Connection",
    platform: "binary_sensor",
    unique_id: "connection-z2",
    state_topic: "simsys/connections/z2/state",
    device_class: "connectivity",
    device: DEVICE,
};
const Z2_CONN_CONFIG_TOPIC: &str = "homeassistant/binary_sensor/simsys/connection_z2/config";

pub fn z2_emeter(influxdb: &InfluxDb) -> TasmotaMeter {
    TasmotaMeter::new(
        influxdb,
        Z2_CONN_CONFIG_TOPIC,
        Z2_CONN_CONFIC,
        Z2_EMETER_CONFIG,
    )
}

const Z3_EMETER_CONFIG: &ConstEMeter = &ConstEMeter {
    e_in: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_e_in_z3/config",
        payload: &Sensor {
            name: "Home Energy",
            platform: "sensor",
            unique_id: "z3-energy-in",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/z3/state",
            availability_topic: "simsys/e_meter/z3/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_in }}",
            suggested_display_precision: 0,
            device: DEVICE
        },
    },
    e_out: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_e_out_z3/config",
        payload: &Sensor {
            name: "Home Energy Output",
            platform: "sensor",
            unique_id: "z3-energy-out",
            enabled_by_default: false,
            state_topic: "simsys/e_meter/z3/state",
            availability_topic: "simsys/e_meter/z3/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_out }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    power: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_power_z3/config",
        payload: &Sensor {
            name: "Home Power",
            platform: "sensor",
            unique_id: "z3-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/z3/state",
            availability_topic: "simsys/e_meter/z3/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.power }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    sec_power: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_sec_power_z3/config",
        payload: &Sensor {
            name: "Home Sec Power",
            platform: "sensor",
            unique_id: "z3-sec-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/z3/state",
            availability_topic: "simsys/e_meter/z3/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.sec_power }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
};

const Z3_CONN_CONFIC: &BinarySensor = &BinarySensor {
    name: "Z3 Connection",
    platform: "binary_sensor",
    unique_id: "connection-z3",
    state_topic: "simsys/connections/z3/state",
    device_class: "connectivity",
    device: DEVICE,
};
const Z3_CONN_CONFIG_TOPIC: &str = "homeassistant/binary_sensor/simsys/connection_z3/config";

pub fn z3_emeter(influxdb: &InfluxDb) -> TasmotaMeter {
    TasmotaMeter::new(
        influxdb,
        Z3_CONN_CONFIG_TOPIC,
        Z3_CONN_CONFIC,
        Z3_EMETER_CONFIG,
    )
}
