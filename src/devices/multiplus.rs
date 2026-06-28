/// Model of a Victron Multiplus 2 device, connected via Modbus TCP
use crate::components::*;
use crate::utils::*;

use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::time::Duration;

use thiserror::Error;
use tokio::time::timeout;
use tokio_modbus::client::{tcp::connect, Context, Reader, Writer};
use tokio_modbus::slave::{Slave, SlaveContext};

const BUS_TIMEOUT: Duration = Duration::from_millis(300);

#[derive(Debug)]
pub struct PiController {
    kp: f64,
    tn: f64,
    ta: f64,
    esum: f64,
    min_u: f64,
    max_u: f64,
}

impl PiController {
    pub fn new(kp: f64, tn: f64, ta: f64, min_u: f64, max_u: f64) -> Self {
        Self {
            kp,
            tn,
            ta,
            esum: 0.0,
            min_u,
            max_u,
        }
    }

    pub fn update(&mut self, error: f64) -> f64 {
        // 1. Integralanteil aufsummieren
        self.esum += error * self.ta;

        // 2. PI-Gleichung
        let mut u = self.kp * (error + (self.esum / self.tn));

        // 3. Anti-Windup / Begrenzung
        if u > self.max_u {
            u = self.max_u;
            self.esum -= error * self.ta;
        } else if u < self.min_u {
            u = self.min_u;
            self.esum -= error * self.ta;
        }
        u
    }
}

#[derive(Error, Debug)]
pub enum ModbusError {
    /// Generic IO errors
    IO(#[from] std::io::Error),

    /// Modbus protocol errors from underlying protocol/transport
    Modbus(#[from] tokio_modbus::Error),

    /// Modbus exception code from server
    ModbusException(#[from] tokio_modbus::ExceptionCode),

    Timeout,
}

impl Display for ModbusError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

const MP2_IP: &str = "192.168.178.173:502";

// Battery soc data point
const BATTERY: u8 = 225;
const STATE_OF_CHARGE: u16 = 266;

// Power set data point
const INVERTER: u8 = 228;
const AC_SET_POWER: u16 = 37;

// Power get data point
const SYSTEM: u8 = 100;
const GRID_POWER: u16 = 820;

/// Internal generic ModbusTCP client
pub struct Multiplus {
    modbus_client: Option<Context>,
    tick_cnt: usize,
    connection: Connection,
    e_meter: EMeter,
    battery: Battery,
    control: Control,
    pi_controller: PiController,
}

#[allow(unused)]
impl Multiplus {
    pub fn new(influxdb: &InfluxDb) -> Self {
        let connection = Connection::new(MULTIPLUS_CONN_CONFIC);
        let e_meter = EMeter::new(
            influxdb,
            MULTIPLUS_EMETER_CONFIG,
        );
        let battery = Battery::new(
            influxdb,
            MULTIPLUS_BATTERY_CONFIG,
        );
        let control = Control::new(
            influxdb,
            MULTIPLUS_CONTROL_CONFIG,
        );
        let pi_controller = PiController::new(-0.65, 4.9, 1.0, -4000.0, 4000.0);

        Multiplus {
            modbus_client: None,
            tick_cnt: TIMEOUT,
            connection,
            e_meter,
            battery,
            control,
            pi_controller,
        }
    }

    pub async fn power_up_msgs(&mut self) -> MqttMessages {
        let mut msgs = self.connection.power_up_msgs();

        msgs += self.e_meter.power_up_msgs().await;
        msgs += self.battery.power_up_msgs().await;
        msgs += self.control.power_up_msgs().await;

        if let Ok(soc) = self.state_of_charge().await {
            msgs += self.battery.set_state_of_charge(soc).await
        }

        msgs
    }

    pub fn get_conn_state_sent(&self) -> bool {
        self.connection.state()
    }

    pub fn e_meter(&self) -> &EMeter {
        &self.e_meter
    }

    pub fn battery(&self) -> &Battery {
        &self.battery
    }

    /// Has to be called once a second
    pub async fn tick_1hz(&mut self) -> MqttMessages {
        // In winter, the Multiplus is switched off. No readings are then displayed. This is a
        // normal state for the system. In this case, the power output is set to 0, which is,
        // of course, consistent with the actual situation. Meanwhile, the connection is set to
        // ‘false’ to reflect the disconnected state, but all values are still available.
        let state = if self.tick_cnt < 10 {
            self.tick_cnt += 1;
            true
        } else {
            false
        };

        // handle the connection ticker with the correct state
        let mut msgs = self.connection.set_state(state);

        // handle e_meter tick_1hz
        msgs += self.e_meter.tick_1hz(state).await;

        // get power value if available or set it to 0.0
        let sec_power = match self.get_mp_power().await {
            Ok(power) => {
                self.tick_cnt = 0;
                power
            }
            // no value from the device? Set it to 0.0!
            Err(_) => 0.0,
        };
        msgs += self.e_meter.set_value(EValue::SecPower { sec_power }).await;

        // calc energy if necessary
        if let Some(power) = self.e_meter.get_new_power() {
            let energy = power * EMETER_MEAN_TIME as f64 / 3.6e6;
            if power <= 0.0 {
                msgs += self
                    .e_meter
                    .set_value(EValue::EnergyInputIncrement { e_in_inc: -energy })
                    .await
            } else {
                msgs += self
                    .e_meter
                    .set_value(EValue::EnergyOutputIncrement { e_out_inc: energy })
                    .await
            }
        }

        if let Ok(soc) = self.state_of_charge().await {
            msgs += self.battery.set_state_of_charge(soc).await
        }

        msgs
    }

    pub async fn state_of_charge(&mut self) -> Result<f64, ModbusError> {
        self.set_unit(BATTERY).await?;
        let soc = self.read_i16(STATE_OF_CHARGE).await?;
        Ok((soc as f64) / 10.0)
    }

    pub async fn get_mp_power(&mut self) -> Result<f64, ModbusError> {
        self.set_unit(SYSTEM).await?;
        let power = self.read_i16(GRID_POWER).await?;
        // positive values: production, negative values consumption
        Ok(-power as f64)
    }

    pub async fn set_power(&mut self, power: f64) -> Result<f64, ModbusError> {
        power.clamp(-4000.0, 4000.0);
        self.set_unit(INVERTER).await?;
        self.write_i16(AC_SET_POWER, power as i16).await?;
        Ok(power)
    }

    pub async fn controller(&mut self, error_p: f64) -> MqttMessages {
        let mut cv = self.pi_controller.update(-error_p);

        // Bat is full -> do not charge anymore
        if self.battery.is_full() && cv > 0.0 {
            cv = 0.0
        }
        // trace!("cv {} grid_p {} pi_control {:?}", cv, grid_p, self.pi_controller);

        // Set inverter power
        let _ = self.set_power(cv).await;

        // Set MQTT Sensor value
        self.control.set_control_value(-cv).await
    }

    async fn get_client(&mut self) -> Result<&mut Context, ModbusError> {
        if self.modbus_client.is_none() {
            let addr: SocketAddr = MP2_IP.parse().unwrap();

            match timeout(BUS_TIMEOUT, connect(addr)).await {
                // Fall 1: Das Timeout ist NICHT abgelaufen (äußeres Ok)
                Ok(connect_result) => match connect_result {
                    // Verbindung war erfolgreich
                    Ok(context) => {
                        self.modbus_client = Some(context);
                    }
                    // Verbindung ist fehlgeschlagen (z.B. Connection Refused)
                    Err(e) => self.on_error().await?,
                },
                // Fall 2: Das Timeout IST abgelaufen (äußeres Err)
                Err(_) => self.on_error().await?, //self.modbus_client = None
            }
        }

        match self.modbus_client.as_mut() {
            Some(context) => Ok(context),
            None => Err(ModbusError::Timeout),
        }
    }

    async fn on_error(&mut self) -> Result<(), ModbusError> {
        self.modbus_client = None;
        self.e_meter
            .set_value(EValue::SecPower { sec_power: 0.0 })
            .await;
        self.e_meter.set_value(EValue::Power { power: 0.0 }).await;
        Err(ModbusError::Timeout)
    }

    async fn set_unit(&mut self, unit: u8) -> Result<(), ModbusError> {
        let client = self.get_client().await?;
        client.set_slave(Slave(unit));
        Ok(())
    }

    async fn write_i16(&mut self, addr: u16, value: i16) -> Result<(), ModbusError> {
        self.write_u16(addr, value as u16).await
    }

    async fn write_u16(&mut self, addr: u16, value: u16) -> Result<(), ModbusError> {
        let client = self.get_client().await?;
        match timeout(BUS_TIMEOUT, client.write_single_register(addr, value)).await {
            Ok(write_result) => {
                write_result??;
                Ok(())
            }
            Err(_) => self.on_error().await,
        }
    }

    async fn read_i16(&mut self, addr: u16) -> Result<i16, ModbusError> {
        Ok(self.read_u16(addr).await? as i16)
    }

    async fn read_u16(&mut self, addr: u16) -> Result<u16, ModbusError> {
        let client = self.get_client().await?;
        match timeout(BUS_TIMEOUT, client.read_input_registers(addr, 1)).await {
            Ok(read_result) => {
                let v = read_result??;
                Ok(v[0])
            }
            Err(_) => {
                self.on_error().await?;
                Err(ModbusError::Timeout)
            }
        }
    }
}

const MULTIPLUS_EMETER_CONFIG: &ConstEMeter = &ConstEMeter {
    e_in: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_e_in_multiplus/config",
        payload: &Sensor {
            name: "Multiplus Energy Charge",
            platform: "sensor",
            unique_id: "multiplus-energy-in",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/multiplus/state",
            availability_topic: "simsys/e_meter/multiplus/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_in }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    e_out: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_e_out_multiplus/config",
        payload: &Sensor {
            name: "Multiplus Energy Discharge",
            platform: "sensor",
            unique_id: "multiplus-energy-out",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/multiplus/state",
            availability_topic: "simsys/e_meter/multiplus/avail",
            unit_of_measurement: "kWh",
            device_class: "energy",
            state_class: "total_increasing",
            value_template: "{{ value_json.e_out }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    power: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_power_multiplus/config",
        payload: &Sensor {
            name: "Multiplus Power",
            platform: "sensor",
            unique_id: "multiplus-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/multiplus/state",
            availability_topic: "simsys/e_meter/multiplus/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.power }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
    sec_power: &SensorConfig {
        topic: "homeassistant/sensor/simsys/e_meter_sec_power_multiplus/config",
        payload: &Sensor {
            name: "Multiplus Sec Power",
            platform: "sensor",
            unique_id: "multiplus-sec-power",
            enabled_by_default: true,
            state_topic: "simsys/e_meter/multiplus/state",
            availability_topic: "simsys/e_meter/multiplus/avail",
            unit_of_measurement: "W",
            device_class: "power",
            state_class: "measurement",
            value_template: "{{ value_json.sec_power }}",
            suggested_display_precision: 0,
            device: DEVICE,
        },
    },
};

const MULTIPLUS_BATTERY_CONFIG: &SensorConfig = &SensorConfig {
    topic: "homeassistant/sensor/simsys/battery_multiplus/config",
    payload: &Sensor {
        name: "Multiplus State of Charge",
        platform: "sensor",
        unique_id: "multiplus-soc",
        enabled_by_default: true,
        state_topic: "simsys/battery/multiplus/state",
        availability_topic: "simsys/e_meter/multiplus/avail",
        unit_of_measurement: "%",
        device_class: "battery",
        state_class: "measurement",
        value_template: "{{ value_json.soc }}",
        suggested_display_precision: 0,
        device: DEVICE,
    }
};

const MULTIPLUS_CONTROL_CONFIG: &SensorConfig = &SensorConfig {
    topic: "homeassistant/sensor/simsys/control_multiplus/config",
    payload: &Sensor {
        name: "Multiplus Control",
        platform: "sensor",
        unique_id: "multiplus-control",
        enabled_by_default: true,
        state_topic: "simsys/control/multiplus/state",
        availability_topic: "simsys/e_meter/multiplus/avail",
        unit_of_measurement: "W",
        device_class: "power",
        state_class: "measurement",
        value_template: "{{ value_json.control_value }}",
        suggested_display_precision: 0,
        device: DEVICE,
    }
};

const MULTIPLUS_CONN_CONFIC: &BinarySensorConfig = &BinarySensorConfig {
    topic: "homeassistant/binary_sensor/simsys/connection_multiplus/config",
    payload: &BinarySensor {
        name: "Multiplus Connection",
        platform: "binary_sensor",
        unique_id: "connection-multiplus",
        state_topic: "simsys/connections/multiplus/state",
        device_class: "connectivity",
        device: DEVICE,
    }
};

pub fn multiplus(influxdb: &InfluxDb) -> Multiplus {
    Multiplus::new(influxdb)
}
