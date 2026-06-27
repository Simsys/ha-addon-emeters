/// Simple Model of a electricity meter of the household consumption
use crate::components::*;
use crate::devices::*;
use crate::utils::*;

pub struct Household {
    e_meter: EMeter,
    tick_cnt: usize,
}

impl Household {
    /// Create a new meter
    pub fn new(influxdb: &InfluxDb) -> Self {
        let e_meter = EMeter::new(
            influxdb,
            HOUSEHOLD_EMETER_CONFIG_TOPIC,
            HOUSEHOLD_EMETER_CONFIG,
        );
        Household {
            e_meter,
            tick_cnt: TIMEOUT,
        }
    }

    pub async fn power_up_msgs(&mut self) -> MqttMessages {
        self.e_meter.power_up_msgs().await
    }

    pub async fn tick_1hz(&mut self) -> MqttMessages {
        let state = if self.tick_cnt < 10 {
            self.tick_cnt += 1;
            true
        } else {
            false
        };
        // handle ticker
        let mut msgs = self.e_meter.tick_1hz(state).await;

        // calc energy if necessary
        if let Some(power) = self.e_meter.get_new_power() {
            let energy = -power * EMETER_MEAN_TIME as f64 / 3.6e6;
            if power < 0.0 {
                msgs += self
                    .e_meter
                    .set_value(EValue::EnergyInputIncrement { e_in_inc: energy })
                    .await
            }
        }
        msgs
    }

    pub async fn set_sec_power(&mut self, sec_power: f64) -> MqttMessages {
        self.tick_cnt = 0;
        self.e_meter.set_value(EValue::SecPower { sec_power }).await
    }
}

const HOUSEHOLD_EMETER_CONFIG: &ConstEMeter = &ConstEMeter {
    device: DEVICE,
    origin: ORIGIN,
    components: &EMeterComponents {
        e_in: &Sensor2 {
            name: "Household Energy",
            platform: "sensor",
            unique_id: "household-energy-in",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/household/state",
            availability_topic: "simsys/e_meter/household/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_in }}",
            suggested_display_precision: 0,
        },
        e_out: &Sensor2 {
            name: "Household Energy Output",
            platform: "sensor",
            unique_id: "household-energy-out",
            enabled_by_default: false,
            state_topic: "simsys/e_meter/household/state",
            availability_topic: "simsys/e_meter/household/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_out }}",
            suggested_display_precision: 0,
        },
        power: &Sensor2 {
            name: "Household Power",
            platform: "sensor",
            unique_id: "household-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/household/state",
            availability_topic: "simsys/e_meter/household/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.power }}",
            suggested_display_precision: 0,
        },
        sec_power: &Sensor2 {
            name: "Household Sec Power",
            platform: "sensor",
            unique_id: "household-sec-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/household/state",
            availability_topic: "simsys/e_meter/household/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.sec_power }}",
            suggested_display_precision: 0,
        },
    },
};
const HOUSEHOLD_EMETER_CONFIG_TOPIC: &str = "homeassistant/device/simsys/e_meter_household/config";

pub fn household(influxdb: &InfluxDb) -> Household {
    Household::new(influxdb)
}
