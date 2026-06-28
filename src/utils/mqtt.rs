use rumqttc::{AsyncClient, ConnectionError, Event, EventLoop, MqttOptions, QoS};
use std::net::ToSocketAddrs;
use std::ops::AddAssign;
use tokio::time::Duration;
use serde::Serialize;

// Defines the maximum age of the values in seconds before they are discarded
pub const TIMEOUT: usize = 10;

#[derive(Debug)]
pub struct MqttMessage {
    topic: String,
    payload: String,
    qos: QoS,
    retain: bool,
}

impl MqttMessage {
    pub fn new(topic: impl Into<String>, payload: impl Into<String>) -> MqttMessage {
        let topic = topic.into();
        let payload = payload.into();
        MqttMessage {
            topic,
            payload,
            qos: QoS::AtMostOnce,
            retain: false,
        }
    }

    #[allow(unused)]
    pub fn set_retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    #[allow(unused)]
    pub fn set_qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    pub fn payload(&self) -> String {
        self.payload.clone()
    }

    pub fn topic(&self) -> String {
        self.topic.clone()
    }

    pub fn qos(&self) -> QoS {
        self.qos
    }

    pub fn retain(&self) -> bool {
        self.retain
    }
}

#[derive(Debug)]
pub struct MqttMessages(Vec<MqttMessage>);

impl AddAssign<MqttMessage> for MqttMessages {
    fn add_assign(&mut self, rhs: MqttMessage) {
        self.0.push(rhs);
    }
}

impl AddAssign<MqttMessages> for MqttMessages {
    fn add_assign(&mut self, mut rhs: MqttMessages) {
        self.0.append(&mut rhs.0);
    }
}

impl MqttMessages {
    pub fn new() -> Self {
        MqttMessages(Vec::new())
    }

    pub fn from_msg(msg: MqttMessage) -> Self {
        MqttMessages(vec![msg])
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub struct MqttConfig {
    pub id: String,
    pub host: String,
    pub username: String,
    pub password: String,
}

pub struct MqttClient {
    client: AsyncClient,
    eventloop: EventLoop,
}

impl MqttClient {
    pub fn new(config: MqttConfig) -> Self {
        let mut addrs = config
            .host
            .to_socket_addrs()
            .expect("Could not read MQTT ip address");
        let (host, port) = {
            let socket = addrs.next().expect("Could not read MQTT ip address");
            (socket.ip().to_string(), socket.port())
        };

        let mut mqttoptions = MqttOptions::new(config.id, host, port);
        mqttoptions
            .set_credentials(config.username, config.password)
            .set_keep_alive(Duration::from_secs(5))
            .set_clean_session(false); // Broker merkt sich Subscriptions (optional)

        // Der Client sendet Befehle, die Eventloop verarbeitet die Netzwerk-Pakete
        let (client, eventloop) = AsyncClient::new(mqttoptions, 200);
        MqttClient { client, eventloop }
    }

    pub async fn publish_msgs(&self, msgs: MqttMessages) {
        for msg in msgs.0 {
            self.client
                .publish(msg.topic(), msg.qos(), msg.retain(), msg.payload())
                .await
                .unwrap()
        }
    }

    #[allow(unused)]
    pub async fn subscribe(&self, topic: &str) {
        self.client.subscribe(topic, QoS::AtMostOnce).await.unwrap();
    }

    pub async fn subscribe_topics(&self, topics: &Vec<&str>) {
        for topic in topics {
            self.client
                .subscribe(*topic, QoS::AtMostOnce)
                .await
                .unwrap();
        }
    }

    pub async fn poll(&mut self) -> Result<Event, ConnectionError> {
        self.eventloop.poll().await
    }
}

// ------------------------------------- MQTT Elements ----------------------------------------

/// Fixed details for the discovery messages
#[allow(unused)]
#[derive(Debug, Serialize)]
pub struct Device {
    identifiers: [&'static str; 1],
    name: &'static str,
    manufacturer: &'static str,
}

/// Fixed details for the discovery messages
#[allow(unused)]
pub const DEVICE: &Device = &Device {
    identifiers: ["ElectricitySensor"],
    name: "Electricity Sensors",
    manufacturer: "SimSys",
};

#[derive(Debug, Serialize)]
pub struct Switch {
    pub name: &'static str,
    pub platform: &'static str,
    pub unique_id: &'static str,
    pub command_topic: &'static str,
    pub state_topic: &'static str,
    pub payload_off: &'static str,
    pub payload_on: &'static str,
    pub device: &'static Device,
}

#[derive(Debug, Serialize)]
pub struct SwitchConfig {
    pub topic: &'static str,
    pub payload: &'static Switch,
}

#[derive(Debug, Serialize)]
pub struct Sensor {
    pub name: &'static str,
    pub platform: &'static str,
    pub unique_id: &'static str,
    pub enabled_by_default: bool,
    pub state_topic: &'static str,
    pub availability_topic: &'static str,
    pub unit_of_measurement: &'static str,
    pub device_class: &'static str,
    pub state_class: &'static str,
    pub value_template: &'static str,
    pub suggested_display_precision: u8,
    pub device: &'static Device,
}

#[derive(Debug, Serialize)]
pub struct SensorConfig {
    pub topic: &'static str,
    pub payload: &'static Sensor,
}

#[derive(Debug, Serialize)]
pub struct BinarySensor {
    pub name: &'static str,
    pub platform: &'static str,
    pub unique_id: &'static str,
    pub state_topic: &'static str,
    pub device_class: &'static str,
    pub device: &'static Device,
}

#[derive(Debug, Serialize)]
pub struct BinarySensorConfig {
    pub topic: &'static str,
    pub payload: &'static BinarySensor,
}

