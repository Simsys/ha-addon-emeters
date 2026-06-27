use crate::{InfluxDbConfig, MqttConfig};
use serde::Deserialize;
use std::fs;

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Deserialize)]
struct Options {
    log_level: String,
    indb_host: String,
    indb_user: String,
    indb_password: String,
    mqtt_host: String,
    mqtt_user: String,
    mqtt_password: String,
}

pub struct Config {
    pub log_level: String,
    pub influx_db_config: InfluxDbConfig,
    pub mqtt_config: MqttConfig,
}

pub fn get_config() -> Config {
    let content = fs::read_to_string("data/options.json").expect("Could not read options.json");
    let options: Options = serde_json::from_str(&content).expect("Could not parse options.json");
    let log_level = options.log_level;
    let influx_db_config = InfluxDbConfig {
        host: options.indb_host,
        user: options.indb_user,
        pass: options.indb_password,
        db: "e_meters".to_string(),
        table: "sensor_data".to_string(),
    };
    let mqtt_config = MqttConfig {
        id: "solar_control".to_string(),
        host: options.mqtt_host,
        username: options.mqtt_user,
        password: options.mqtt_password,
    };
    Config {
        log_level,
        influx_db_config,
        mqtt_config,
    }
}
