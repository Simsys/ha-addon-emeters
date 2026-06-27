mod components;
mod devices;
mod utils;

use crate::utils::{MqttConfig, MqttMessages};
use crate::{components::*, devices::*};
use utils::*;

use log::*;
use rumqttc::{Event, Packet};
use tokio::{
    time,
    time::{interval, Duration},
};
use utils::MqttClient;

#[tokio::main]
async fn main() {
    // get config
    let config = get_config();

    // set log level
    let rust_log = format!("error,solar_control={}", config.log_level);
    std::env::set_var("RUST_LOG", rust_log);
    env_logger::init();

    // show app name and version
    info!("{} Version {}", APP_NAME, APP_VERSION);

    // initialise influxdb and mqtt client
    let influxdb = InfluxDb::new(config.influx_db_config);
    let mut mqtt_client = MqttClient::new(config.mqtt_config);

    let mut devices = Devices::new(&influxdb);
    let mut ticker = interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let msgs = devices.tick_1hz().await;
                mqtt_client.publish_msgs(msgs).await;
            }

            event = mqtt_client.poll() => {
                match event {
                    Ok(notification) => {
                        match notification {
                            Event::Incoming(Packet::Publish(publish)) => {
                                let msgs = devices.process_msgs(&publish).await;
                                if msgs.is_empty() {
                                    if publish.topic.as_str() == "homeassistant/status" {
                                        if publish.payload == "online" {
                                            // homeassistant has now completly started
                                            let msgs = devices.power_up_msgs().await;
                                            mqtt_client.publish_msgs(msgs).await;
                                            info!("HA now online, sent power up messages again");
                                        }
                                    }
                                } else {
                                    mqtt_client.publish_msgs(msgs).await;
                                }
                            }
                            Event::Incoming(Packet::ConnAck(_connack)) => {
                                trace!("MQTT connection acknowledged");

                                // subscribe to MQTT topics
                                let mut topics = devices.subscribe_topics();
                                topics.push("homeassistant/status");
                                mqtt_client.subscribe_topics(&topics).await;

                                // send power up Messages
                                let msgs = devices.power_up_msgs().await;
                                mqtt_client.publish_msgs(msgs).await;
                                info!("Initialising completed");
                            }
                            _ => (),
                        }
                    }
                    Err(e) => {
                        error!("Error: {:?}", e);
                        time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }
}
