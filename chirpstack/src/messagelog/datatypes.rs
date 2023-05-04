use crate::config;

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
    OK,
    #[default]
    NOK,
    WARN,
}

#[derive(Debug, Default)]
pub struct LogEntryBuilder {
    pub created_at: DateTime<Utc>,
    pub log_source: Endpoint,
    pub source_id: String,
    pub log_destination: Endpoint,
    pub destination_id: String, // String is the wrong type here, we should use NetID
}

impl LogEntryBuilder {
    pub fn new() -> Self {
        LogEntryBuilder {
            created_at: Utc::now(),
            ..Default::default()
        }
    }
    pub fn log_source(mut self, v: Endpoint) -> Self {
        self.log_source = v;
        self
    }
    pub fn log_destination(mut self, v: Endpoint) -> Self {
        self.log_destination = v;
        self
    }

    pub fn our_desitnation_id(mut self) -> Self {
        let conf = config::get();
        self.destination_id = conf.network.net_id.to_string();
        self
    }

    pub fn source_id(mut self, v: impl Into<String>) -> Self {
        self.source_id = v.into();
        self
    }

    pub fn build(self) -> LogEntry {
        LogEntry {
            created_at: self.created_at,
            log_source: self.log_source,
            source_id: self.source_id,
            log_destination: self.log_destination,
            destination_id: self.destination_id,
            ..Default::default()
        }
    }
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

#[cfg(test)]
mod test {
    use super::*;
    use lrwn::DevAddr;
    use std::error::Error;
    use std::str::FromStr;
    use uuid::uuid;

    #[test]
    fn test_json_format() -> Result<(), Box<dyn Error>> {
        let orig = LogEntry {
            publish_at: DateTime::parse_from_rfc3339("2023-05-03T11:58:41.21027935+02:00")?.into(),
            dev_addr: DevAddr::from_str("00000000")?,
            ctx_id: uuid!("c2864e43-174a-42a9-a8a5-71b0bd87b644"),
            known_device: true,
            time_on_air: 0.0,
            created_at: DateTime::parse_from_rfc3339("2023-05-03T11:58:41.204119632+02:00")?.into(),
            dev_eui: EUI64::from_str("0080e1150044bb7a")?,
            source_id: "600002".into(),
            log_destination: Endpoint::Gateway,
            frame_status: FrameStatus {
                error_desc: "".to_string(),
                result: FrameStatusResult::NOK,
            },
            log_source: Endpoint::Local,
            destination_id: "647fdafffe00c7bb".into(),
        };

        let encoded = serde_json::to_string_pretty(&orig)?;
        println!("{encoded}");

        Ok(())
    }
    const EXPECTED_01: &str = r#"{
  "PublishAt": "2023-05-03T11:58:41.21027935+02:00",
  "DevAddr": "00000000",
  "CtxID": "c2864e43-174a-42a9-a8a5-71b0bd87b644",
  "KnownDevice": true,
  "CreatedAt": "2023-05-03T11:58:41.204119632+02:00",
  "DevEUI": "0080e1150044bb7a",
  "SourceID": "600002",
  "LogDestination": "GATEWAY",
  "FrameStatus": {
    "ErrorDesc": "",
    "Result": "NOK"
  },
  "TimeOnAir": 0,
  "LogSource": "LOCAL",
  "DestinationID": "647fdafffe00c7bb"
}"#;
}
