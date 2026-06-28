use log::*;
/// Define all devices / electricity meters
mod heatpump;
pub use heatpump::{heatpump, Heatpump};

mod household;
pub use household::{household, Household};

mod multiplus;
pub use multiplus::{multiplus, Multiplus};

//mod tasmota_meter;
//pub use tasmota_meter::TasmotaMeter;

mod tasmota_meters;
pub use tasmota_meters::{z1_emeter, z2_emeter, z3_emeter, TasmotaMeter};

mod wallbox;
pub use wallbox::*;

// Definitions
//
// The following section defines which devices exist, what their names are, and what units they
// use. It also specifies how signs are applied.
//
// Heatpump ok
// - power: Consumption is viewed negatively, heatpump-power = grid-power - home-power
// - e_in: Energy consumed
//
// Household ok
// - power: Consumption is viewed negatively
// - e_in: Energy consumed
//
// Multiplus ok
// - power: Positive values – battery is discharging; negative values – battery is charging
// - e_in: Energy flows into the home battery
// - e_out: The home battery powers the house
//
// Z1 / Home Power ok
// - power: home-power = household-power + solar-power + wallbox-power + multiplus-power
// - e_in: energy consumpton
//
// Z2 / Grid ok
// - power: Positive values - energy flow to grid, negative values - energy consumption
// - e_in: Energy input (consumption)
// - e_out: Energy output (production)
//
// Z3 / Solar ok
// - power: Positive values - production, negative values - never
// - e_out: Energy production
//
// Wallbox
// - power: Negative values - consumption, positive values - never
// - e_in: Energy flows into the car battery

use crate::{InfluxDb, MqttMessages};
use rumqttc::Publish;

pub struct Devices {
    z1: TasmotaMeter,
    z2: TasmotaMeter,
    z3: TasmotaMeter,
    multiplus: Multiplus,
    household: Household,
    heatpump: Heatpump,
    wallbox: Wallbox,
    cnt: u32,
    info_online: bool,
}

impl Devices {
    pub fn new(influxdb: &InfluxDb) -> Self {
        Devices {
            z1: z1_emeter(influxdb),
            z2: z2_emeter(influxdb),
            z3: z3_emeter(influxdb),
            multiplus: multiplus(influxdb),
            household: household(influxdb),
            heatpump: heatpump(influxdb),
            wallbox: wallbox_meter(influxdb),
            cnt: 0,
            info_online: false,
        }
    }

    pub async fn power_up_msgs(&mut self) -> MqttMessages {
        let mut msgs = self.heatpump.power_up_msgs().await;
        msgs += self.household.power_up_msgs().await;
        msgs += self.multiplus.power_up_msgs().await;
        msgs += self.wallbox.power_up_msgs().await;
        msgs += self.z1.power_up_msgs().await;
        msgs += self.z2.power_up_msgs().await;
        msgs += self.z3.power_up_msgs().await;
        msgs
    }

    pub fn subscribe_topics(&self) -> Vec<&'static str> {
        vec![
            "go-eCharger/083205/car",
            "go-eCharger/083205/eto",
            "go-eCharger/083205/nrg",
            "tasmota/electricity-meter/+/SENSOR",
            "simsys/e_meter/wallbox/no_solar_cmd",
        ]
    }

    pub async fn tick_1hz(&mut self) -> MqttMessages {
        // handle e-meter ticker
        let mut msgs = self.z1.tick_1hz().await;
        msgs += self.z2.tick_1hz().await;
        msgs += self.z3.tick_1hz().await;
        msgs += self.heatpump.tick_1hz().await;
        msgs += self.household.tick_1hz().await;
        msgs += self.multiplus.tick_1hz().await;
        msgs += self.wallbox.tick_1hz().await;

        // get multiplus power if awailable
        let mut multiplus_p = 0.0;
        if let Some(power) = self.multiplus.e_meter().get_sec_power() {
            multiplus_p = power;
        }

        // calculate missing values
        if let (Some(home_p), Some(grid_p), Some(solar_p), Some(wallbox_p)) = (
            self.z1.e_meter().get_sec_power(),
            self.z2.e_meter().get_sec_power(),
            self.z3.e_meter().get_sec_power(),
            self.wallbox.e_meter().get_sec_power(),
        ) {
            let heatpump_p = grid_p - home_p;
            msgs += self.heatpump.set_sec_power(heatpump_p).await;

            let household_p = home_p - solar_p - wallbox_p - multiplus_p;
            msgs += self.household.set_sec_power(household_p).await;

            // Call car controller with power available
            let mut avail_p = grid_p - multiplus_p - wallbox_p;
            if avail_p < 1600.0 && self.multiplus.battery().enough_for_car() {
                // if battery has enough energy for car, use it to charge it
                avail_p += 3000.0;
            }
            msgs += self.wallbox.control(avail_p).await;

            // Call Homebat controller, try to neutralise power consumption
            let error_p = grid_p + if solar_p > 1000.0 { -100.0 } else { -20.0 };
            msgs += self.multiplus.controller(error_p).await;
        }

        let mut ok_conns: Vec<&str> = Vec::new();
        let mut error_cons: Vec<&str> = Vec::new();
        if self.z1.get_conn_state_sent() {
            ok_conns.push("z1")
        } else {
            error_cons.push("z1");
        }
        if self.z2.get_conn_state_sent() {
            ok_conns.push("z2")
        } else {
            error_cons.push("z2");
        }
        if self.z3.get_conn_state_sent() {
            ok_conns.push("z3")
        } else {
            error_cons.push("z3");
        }
        if self.multiplus.get_conn_state_sent() {
            ok_conns.push("multiplus")
        } else {
            error_cons.push("multiplus");
        }
        if self.wallbox.get_conn_state_sent() {
            ok_conns.push("wallbox")
        } else {
            error_cons.push("wallbox");
        }

        self.cnt = (self.cnt + 1) & 0x07;
        if self.cnt & 0x07 == 0x07 {
            if error_cons.is_empty() {
                if !self.info_online {
                    info!("All devices are online");
                    self.info_online = true;
                }
            } else {
                error!(
                    "Devices offline: {}; online: {}",
                    error_cons.join(", "),
                    ok_conns.join(", ")
                );
                self.info_online = false;
            }
        }

        msgs
    }

    pub async fn process_msgs(&mut self, publish: &Publish) -> MqttMessages {
        let payload = &publish.payload;
        match publish.topic.as_str() {
            "tasmota/electricity-meter/z1/SENSOR" => self.z1.value_from_json(payload).await,
            "tasmota/electricity-meter/z2/SENSOR" => self.z2.value_from_json(payload).await,
            "tasmota/electricity-meter/z3/SENSOR" => self.z3.value_from_json(payload).await,
            "go-eCharger/083205/eto" => self.wallbox.value_from_eto(payload).await,
            "go-eCharger/083205/nrg" => self.wallbox.value_from_nrg(payload).await,
            "go-eCharger/083205/car" => self.wallbox.value_from_car(payload).await,
            "simsys/e_meter/wallbox/no_solar_cmd" => self.wallbox.no_solar_switch(payload).await,

            _ => MqttMessages::new(),
        }
    }
}
