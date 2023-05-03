use tracing::info;

use anyhow::Result;

mod backend;
mod datatypes;

use crate::config;
pub use datatypes::{Endpoint, FrameStatus, FrameStatusResult, LogEntry};

use self::backend::mqtt::MqttBackend;
use tokio::sync::RwLock;

lazy_static! {
    static ref BACKEND: RwLock<Option<MqttBackend>> = RwLock::new(None);
}

pub async fn setup() -> Result<()> {
    let conf = config::get();
    if conf.message_logger.mqtt.servers.is_empty() {
        info!("Message logger disabled.");
    } else {
        let mqtt_backend = MqttBackend::new(&conf.message_logger.mqtt).await?;
        {
            let mut backend = BACKEND.write().await;
            *backend = Some(mqtt_backend);
        }
    }
    Ok(())
}
