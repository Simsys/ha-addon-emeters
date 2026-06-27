/// Connection to a server: create MQTT discovery message and state messages
use crate::{
    utils::{BinarySensor, MqttMessage, MqttMessages},
};
use log::*;

/// The connection struct chechs and signals connection to a remote server
pub struct Connection {
    config_topic: &'static str,
    config: &'static BinarySensor,
    state_sent: Option<bool>,
}

impl Connection {
    /// Create a connection struct
    pub fn new(config_topic: &'static str, config: &'static BinarySensor) -> Self {
        Connection {
            config_topic,
            config,
            state_sent: None,
        }
    }

    /// Create discovery message
    pub fn power_up_msgs(&self) -> MqttMessages {
        let payload = serde_json::to_string(self.config).unwrap();
        let msg = MqttMessage::new(self.config_topic, payload)
            .set_qos(rumqttc::QoS::AtLeastOnce)
            .set_retain(true);
        MqttMessages::from_msg(msg)
    }

    /// Set state of the connection. It is usually checked whether data is being received from
    /// the server on a regular basis
    pub fn set_state(&mut self, state: bool) -> MqttMessages {
        match self.state_sent {
            Some(state_sent) => {
                if state != state_sent {
                    self.state_sent = Some(state);
                    self.create_message()
                } else {
                    MqttMessages::new()
                }
            }
            None => {
                // This ensures that the current status is actually sent after the power-up
                self.state_sent = Some(state);
                self.create_message()
            }
        }
    }

    /// Get the state of the connection
    #[allow(unused)]
    pub fn state(&self) -> bool {
        if let Some(state) = self.state_sent {
            state
        } else {
            false
        }
    }

    fn create_message(&self) -> MqttMessages {
        let topic = self.config.state_topic;
        let (payload, msg) = if self.state() {
            ("ON", "Device connected")
        } else {
            ("OFF", "Connection lost")
        };
        trace!("{} {}", msg, topic);
        MqttMessages::from_msg(
            MqttMessage::new(topic, payload)
                .set_retain(true)
                .set_retain(true)
                .set_retain(true),
        )
    }
}
