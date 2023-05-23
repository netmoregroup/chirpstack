use std::collections::HashMap;

// Traits
use anyhow::Result;
use prost::Message;
use serde::Serialize;

use async_trait::async_trait;
use handlebars::Handlebars;
use pulsar::Pulsar;
use tracing::{info, trace};

use super::Integration as IntegrationTrait;
use crate::config::PulsarIntegration as Config;
use chirpstack_api::integration;

pub struct Integration<'templates> {
    client: Pulsar<pulsar::executor::TokioExecutor>,
    templates: Handlebars<'templates>,
    json: bool,
}

#[derive(Serialize)]
struct EventTopicContext {
    pub application_id: String,
    pub dev_eui: String,
    pub event: String,
}

impl<'templates> Integration<'templates> {
    pub async fn new(conf: &Config) -> Result<Integration<'templates>> {
        use pulsar::Authentication;

        info!("Initializing Pulsar integration");
        // topic templates
        let mut templates = Handlebars::new();
        templates.register_escape_fn(handlebars::no_escape);
        templates.register_template_string("event_topic", &conf.event_topic)?;

        let mut builder = Pulsar::builder(conf.server.clone(), pulsar::executor::TokioExecutor);

        // JWT authentication
        if !conf.auth_token.is_empty() {
            let auth = Authentication {
                name: "token".to_string(),
                data: conf.auth_token.clone().into_bytes(),
            };
            builder = builder.with_auth(auth);
        }
        let client = builder.build().await?;
        Ok(Integration {
            client,
            templates,
            json: conf.json,
        })
    }

    async fn publish_event(&self, topic: &str, payload: Vec<u8>) -> Result<()> {
        info!(topic = %topic, "Publishing event");

        let msg = pulsar::producer::Message {
            payload,
            ..Default::default()
        };

        // Rather than keeping track of producers per-topic, we use the built-in "lazy" option to
        // do so. Less control of schema and other producer options, but simpler implementation.
        let acked = self.client.send(topic, msg).await?;
        // Ack waiting is not mandatory, and can take an arbitrary amount of time, as there may be
        // batching and more happening.
        // In 2022 context, it is okay as events spawn in their own tasks and don't block other
        // progress, however, if that changes, this may require some attention
        trace!(topic = %topic, "Waiting for ack");
        acked.await?;
        Ok(())
    }

    fn get_event_topic(&self, application_id: &str, dev_eui: &str, event: &str) -> Result<String> {
        let topic = self.templates.render(
            "event_topic",
            &EventTopicContext {
                application_id: application_id.to_string(),
                dev_eui: dev_eui.to_string(),
                event: event.to_string(),
            },
        )?;
        Ok(topic)
    }
}

#[async_trait]
impl<'templates> IntegrationTrait for Integration<'templates> {
    async fn uplink_event(
        &self,
        _vars: &HashMap<String, String>,
        pl: &integration::UplinkEvent,
    ) -> Result<()> {
        let dev_info = pl
            .device_info
            .as_ref()
            .ok_or_else(|| anyhow!("device_info is None"))?;
        let topic = self.get_event_topic(&dev_info.application_id, &dev_info.dev_eui, "up")?;

        let payload = match self.json {
            true => serde_json::to_vec(&pl)?,
            false => pl.encode_to_vec(),
        };
        self.publish_event(&topic, payload).await
    }

    async fn join_event(
        &self,
        _vars: &HashMap<String, String>,
        pl: &integration::JoinEvent,
    ) -> Result<()> {
        let dev_info = pl
            .device_info
            .as_ref()
            .ok_or_else(|| anyhow!("device_info is None"))?;

        let topic = self.get_event_topic(&dev_info.application_id, &dev_info.dev_eui, "join")?;
        let payload = match self.json {
            true => serde_json::to_vec(&pl)?,
            false => pl.encode_to_vec(),
        };

        self.publish_event(&topic, payload).await
    }

    async fn ack_event(
        &self,
        _vars: &HashMap<String, String>,
        pl: &integration::AckEvent,
    ) -> Result<()> {
        let dev_info = pl
            .device_info
            .as_ref()
            .ok_or_else(|| anyhow!("device_info is None"))?;

        let topic = self.get_event_topic(&dev_info.application_id, &dev_info.dev_eui, "ack")?;
        let payload = match self.json {
            true => serde_json::to_vec(&pl)?,
            false => pl.encode_to_vec(),
        };

        self.publish_event(&topic, payload).await
    }

    async fn txack_event(
        &self,
        _vars: &HashMap<String, String>,
        pl: &integration::TxAckEvent,
    ) -> Result<()> {
        let dev_info = pl
            .device_info
            .as_ref()
            .ok_or_else(|| anyhow!("device_info is None"))?;

        let topic = self.get_event_topic(&dev_info.application_id, &dev_info.dev_eui, "txack")?;
        let payload = match self.json {
            true => serde_json::to_vec(&pl)?,
            false => pl.encode_to_vec(),
        };

        self.publish_event(&topic, payload).await
    }

    async fn log_event(
        &self,
        _vars: &HashMap<String, String>,
        pl: &integration::LogEvent,
    ) -> Result<()> {
        let dev_info = pl
            .device_info
            .as_ref()
            .ok_or_else(|| anyhow!("device_info is None"))?;

        let topic = self.get_event_topic(&dev_info.application_id, &dev_info.dev_eui, "log")?;
        let payload = match self.json {
            true => serde_json::to_vec(&pl)?,
            false => pl.encode_to_vec(),
        };

        self.publish_event(&topic, payload).await
    }

    async fn status_event(
        &self,
        _vars: &HashMap<String, String>,
        pl: &integration::StatusEvent,
    ) -> Result<()> {
        let dev_info = pl
            .device_info
            .as_ref()
            .ok_or_else(|| anyhow!("device_info is None"))?;

        let topic = self.get_event_topic(&dev_info.application_id, &dev_info.dev_eui, "status")?;
        let payload = match self.json {
            true => serde_json::to_vec(&pl)?,
            false => pl.encode_to_vec(),
        };

        self.publish_event(&topic, payload).await
    }

    async fn location_event(
        &self,
        _vars: &HashMap<String, String>,
        pl: &integration::LocationEvent,
    ) -> Result<()> {
        let dev_info = pl
            .device_info
            .as_ref()
            .ok_or_else(|| anyhow!("device_info is None"))?;

        let topic =
            self.get_event_topic(&dev_info.application_id, &dev_info.dev_eui, "location")?;
        let payload = match self.json {
            true => serde_json::to_vec(&pl)?,
            false => pl.encode_to_vec(),
        };

        self.publish_event(&topic, payload).await
    }

    async fn integration_event(
        &self,
        _vars: &HashMap<String, String>,
        pl: &integration::IntegrationEvent,
    ) -> Result<()> {
        let dev_info = pl
            .device_info
            .as_ref()
            .ok_or_else(|| anyhow!("device_info is None"))?;

        let topic =
            self.get_event_topic(&dev_info.application_id, &dev_info.dev_eui, "integration")?;
        let payload = match self.json {
            true => serde_json::to_vec(&pl)?,
            false => pl.encode_to_vec(),
        };

        self.publish_event(&topic, payload).await
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::test;
    use futures::TryStreamExt;
    use regex::Regex;
    use std::time::Duration;
    use tokio::time::timeout;
    use tracing::{debug, trace};

    use pulsar::message::proto::command_subscribe::SubType;
    use uuid::Uuid;
    #[tokio::test]
    async fn test_pulsar() {
        let _guard = test::prepare().await;
        let conf = Config {
            server: "pulsar://pulsar:6650".to_string(),
            json: true,
            ..Default::default()
        };
        let pulsar = Pulsar::builder(conf.server.clone(), pulsar::executor::TokioExecutor)
            .build()
            .await
            .unwrap();

        let topic_re = Regex::new(r".*application\..*").unwrap();
        let topic =
            "application.00000000-0000-0000-0000-000000000000.device.0102030405060708.event.up";

        let mut consumer: pulsar::Consumer<Vec<u8>, _> = pulsar
            .consumer()
            .with_consumer_name("test_consumer")
            // Regexp-only topics only work if the pulsar server HAS the topics first.
            // So for a test-case where the topic doesn't exist until _after_ the first publish,
            // we may not see it.
            // This means that the first time tests run, they fail, but succeed afterwards, which
            // is bad.
            .with_topic(topic)
            .with_topic_regex(topic_re)
            .with_subscription("test_subscription")
            .with_subscription_type(SubType::Exclusive)
            .build()
            .await
            .expect("Failed to create consumer");

        info!(what = "pulsar_testcase", "Consumer created");
        // Check that we have a connection before testing
        consumer
            .check_connection()
            .await
            .expect("Consumer connection is not healthy");

        let pl = integration::UplinkEvent {
            device_info: Some(integration::DeviceInfo {
                application_id: Uuid::nil().to_string(),
                dev_eui: "0102030405060708".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let expected = serde_json::to_vec(&pl).unwrap();

        // Spawn a listener before we send messages.
        let handle = tokio::spawn(async move {
            debug!(what = "consumer", "Consumer waiting for event");
            while let Some(msg) = consumer.try_next().await.unwrap() {
                debug!(what = "consumer", topic = &msg.topic, "Event received");
                assert_eq!(
                    "persistent://public/default/application.00000000-0000-0000-0000-000000000000.device.0102030405060708.event.up",
                    msg.topic
                );
                assert_eq!(expected, msg.payload.data);
                debug!(what = "consumer", "Acking Event received");
                consumer.ack(&msg).await.expect("Failed to ack");
                break;
            }
            consumer
                .close()
                .await
                .expect("Failed to unsubscribe consumer");
        });

        let i = Integration::new(&conf).await.unwrap();
        trace!(what = "pulsar_testcase", "Integration created");

        i.uplink_event(&HashMap::new(), &pl).await.unwrap();
        debug!(what = "pulsar_testcase", "Event published");

        let max_delay = Duration::from_secs(60);
        info!(
            what = "pulsar_testcase",
            delay = ?max_delay,
            "Waiting for result"
        );
        let _ = timeout(max_delay, handle)
            .await
            .expect("Timeout waiting for event data");
    }
}
