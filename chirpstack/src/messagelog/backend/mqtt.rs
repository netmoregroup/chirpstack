use std::time::Duration;

use anyhow::{Context, Result};
use paho_mqtt as mqtt;
use rand::Rng;
use tracing::{error, info, trace};

use crate::config::MessageLoggerBackendMqtt;

use crate::messagelog;

pub struct MqttBackend {
    client: mqtt::AsyncClient,
    topic: String,
    qos: usize,
}

impl MqttBackend {
    pub async fn new(conf: &MessageLoggerBackendMqtt) -> Result<MqttBackend> {
        let topic = conf.log_topic.clone();
        // get client id, this will generate a random client_id when no client_id has been
        // configured.
        let client_id = if conf.client_id.is_empty() {
            let mut rnd = rand::thread_rng();
            let client_id: u64 = rnd.gen();
            format!("{:x}", client_id)
        } else {
            conf.client_id.clone()
        };

        // create client
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .client_id(&client_id)
            .finalize();
        let client = mqtt::AsyncClient::new(create_opts).context("Create MQTT client")?;

        client.set_connected_callback(|_client| {
            info!("MQTT connection to messagelog backend.");
        });
        client.set_connection_lost_callback(|_client| {
            error!("MQTT connection to messagelog backend lost");
        });

        // connection options
        let mut conn_opts_b = mqtt::ConnectOptionsBuilder::new();
        conn_opts_b.server_uris(&conf.servers);
        conn_opts_b.automatic_reconnect(Duration::from_secs(1), Duration::from_secs(30));
        conn_opts_b.clean_session(conf.clean_session);
        conn_opts_b.keep_alive_interval(conf.keep_alive_interval);
        conn_opts_b.user_name(&conf.username);
        conn_opts_b.password(&conf.password);
        if !conf.ca_cert.is_empty() || !conf.tls_cert.is_empty() || !conf.tls_key.is_empty() {
            info!(
                ca_cert = conf.ca_cert.as_str(),
                tls_cert = conf.tls_cert.as_str(),
                tls_key = conf.tls_key.as_str(),
                "Configuring connection with TLS certificate"
            );

            let mut ssl_opts_b = mqtt::SslOptionsBuilder::new();

            if !conf.ca_cert.is_empty() {
                ssl_opts_b
                    .trust_store(&conf.ca_cert)
                    .context("Failed to set gateway ca_cert")?;
            }

            if !conf.tls_cert.is_empty() {
                ssl_opts_b
                    .key_store(&conf.tls_cert)
                    .context("Failed to set gateway tls_cert")?;
            }

            if !conf.tls_key.is_empty() {
                ssl_opts_b
                    .private_key(&conf.tls_key)
                    .context("Failed to set gateway tls_key")?;
            }
            conn_opts_b.ssl_options(ssl_opts_b.finalize());
        }
        let conn_opts = conn_opts_b.finalize();

        let b = MqttBackend {
            client,
            topic,
            qos: conf.qos,
        };

        // connect
        info!(clean_session = conf.clean_session, client_id = %client_id, "Connecting to MQTT broker");
        b.client
            .connect(conn_opts)
            .await
            .context("Connect to MQTT broker")?;

        // return backend
        Ok(b)
    }

    pub async fn log_message(&self, log_entry: messagelog::LogEntry) -> Result<()> {
        let payload = serde_json::to_vec(&log_entry)?;
        info!(topic = %self.topic, "Sending log message");
        let msg = mqtt::Message::new(&self.topic, payload, self.qos as i32);
        self.client.publish(msg).await?;
        trace!("Log message sent");
        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::test;

    use futures::stream::StreamExt;

    use paho_mqtt as mqtt;

    // Helper to get a listen-able client for the test-case
    async fn make_mqtt_client() -> mqtt::AsyncClient {
        // Set up a mqtt client
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri("tcp://mosquitto:1883/")
            .finalize();
        let client = mqtt::AsyncClient::new(create_opts).unwrap();
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .clean_session(true)
            .finalize();
        client.connect(conn_opts).await.unwrap();

        client
            .subscribe("messagelog/testcase", mqtt::QOS_0)
            .await
            .unwrap();
        client
    }

    #[tokio::test]
    async fn test_messagelog() {
        // We don't use the backend here, as the test-case is only really testing the MQTT
        // connection currently.
        //
        // It should be increased to test/verify our expected events as well.
        let _guard = test::prepare().await;
        let conf = MessageLoggerBackendMqtt {
            log_topic: "messagelog/testcase".into(),
            servers: vec!["tcp://mosquitto:1883/".into()],
            ..Default::default()
        };
        let mqtt_backend = MqttBackend::new(&conf).await.unwrap();
        let mut client = make_mqtt_client().await;
        let mut stream = client.get_stream(10);

        // TODO: Spindel,  add test-case for Roaming
        //      backend.PRStartReq
        //      backend.XmitDataReq
        //      handlePRStartAns
        //      handlePRStartReq
        //      handlePRStartReqData

        // TODO: Spindel, add test-case for HandleDownlinkTXAck (downlink /ack )
        //  forJoinAcceptPayload
        //
        //
        //
        // uplink event

        // TODO: Spindel, add test-case for HandleDownlinkTXAck (downlink /ack )
        //

        let log_entry = messagelog::LogEntry::default();
        let expected = serde_json::to_string(&log_entry).unwrap();
        mqtt_backend.log_message(log_entry).await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.payload_str(), expected);
    }
}
