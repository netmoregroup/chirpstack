use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use lrwn::EUI64;

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct FrameStatus {
    pub result: FrameStatusResult,
    pub error_desc: String,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum Endpoint {
    Gateway,
    #[default]
    Local,
    Roaming,
    JoinServer,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Serialize, Default)]
pub enum FrameStatusResult {
    #[default]
    OK,
    NOK,
    WARN,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct LogEntry {
    #[serde(rename = "CtxID")]
    pub ctx_id: Uuid,
    pub publish_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub log_source: Endpoint,
    #[serde(rename = "SourceID")]
    pub source_id: String,
    pub log_destination: Endpoint,
    #[serde(rename = "DestinationID")]
    pub destination_id: String,
    pub frame_status: FrameStatus,
    pub time_on_air: f64,
    pub dev_addr: lrwn::DevAddr,
    #[serde(rename = "DevEUI")]
    pub dev_eui: EUI64, // backend uses Vec<u8> here with a hex_encode encoder.
    pub known_device: bool,
}
