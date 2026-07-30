//! UDP JSON v1 消息模型和数据报入口校验。

use std::collections::BTreeMap;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::{Uuid, Variant, Version};

use super::{
    MetricPoint, PerformanceDomain, StreamKind, StreamOutcome, StreamParameter, ValidationError,
    validate_metric_for_domain,
};

/// v1 单个 UDP 数据报的最大字节数。
pub const MAX_DATAGRAM_BYTES: usize = 1200;

/// JSON 安全整数允许的最大序列和时间值。
pub const MAX_WIRE_INTEGER: u64 = i64::MAX as u64;

/// 单个快照批次允许的最大分片数。
pub const MAX_SNAPSHOT_PARTS: u32 = 256;

/// 单个指标窗口允许的最大时长。
pub const MAX_WINDOW_DURATION_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// 安装级设备身份。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceIdentity {
    /// 持久化随机 UUID。
    pub id: String,
    /// 当前操作系统主机名。
    pub name: Option<String>,
}

/// FluxTerm 进程来源。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Source {
    /// 固定应用名。
    pub application: String,
    /// 应用版本。
    pub version: String,
    /// 随机进程实例 ID。
    pub instance_id: String,
    /// 安装级设备身份。
    pub device: DeviceIdentity,
    /// 平台。
    pub platform: String,
    /// CPU 架构。
    pub arch: String,
    /// 构建类型。
    pub build_profile: String,
}

/// 性能流的远程连接目标。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamTarget {
    /// 数字 IP 或 ASCII 主机名。
    pub host: String,
    /// 远程服务端口。
    pub port: u16,
}

/// 业务会话与传输任务关联。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamCorrelation {
    /// SSH 或 RDP 会话 UUID。
    pub session_id: String,
    /// SFTP 传输任务 ID。
    pub transfer_id: Option<String>,
}

/// 流打开消息中的完整描述。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamDescriptor {
    /// 匿名流 ID。
    pub id: String,
    /// 业务域。
    pub domain: PerformanceDomain,
    /// 流类型。
    pub kind: StreamKind,
    /// 流开始时间。
    pub started_at_unix_ms: u64,
    /// 低基数性能参数。
    pub parameters: BTreeMap<String, StreamParameter>,
    /// 远程连接目标。
    pub target: StreamTarget,
    /// 业务关联。
    pub correlation: StreamCorrelation,
}

/// 快照和关闭消息中的流引用。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamReference {
    /// 匿名流 ID。
    pub id: String,
    /// 业务域。
    pub domain: PerformanceDomain,
    /// 流类型。
    pub kind: StreamKind,
    /// 远程连接目标。
    pub target: StreamTarget,
    /// 业务关联。
    pub correlation: StreamCorrelation,
}

/// 指标聚合窗口。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MetricWindow {
    /// 窗口开始时间。
    pub started_at_unix_ms: u64,
    /// 窗口长度。
    pub duration_ms: u64,
}

/// 流关闭信息。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClosedBody {
    /// 稳定关闭结果。
    pub outcome: StreamOutcome,
    /// 流结束时间。
    pub ended_at_unix_ms: u64,
}

/// 流打开消息。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OpenedMessage {
    /// 协议版本。
    pub schema_version: u8,
    /// 进程来源。
    pub source: Source,
    /// 完整流描述。
    pub stream: StreamDescriptor,
    /// 流内数据报序列。
    pub sequence: u64,
    /// 发送时间。
    pub sent_at_unix_ms: u64,
}

/// 指标快照消息。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotMessage {
    /// 协议版本。
    pub schema_version: u8,
    /// 进程来源。
    pub source: Source,
    /// 流引用。
    pub stream: StreamReference,
    /// 流内数据报序列。
    pub sequence: u64,
    /// 拆包批次 ID。
    pub batch_id: String,
    /// 当前分片下标。
    pub part_index: u32,
    /// 总分片数。
    pub part_count: u32,
    /// 发送时间。
    pub sent_at_unix_ms: u64,
    /// 指标窗口。
    pub window: MetricWindow,
    /// 完整指标点。
    pub metrics: Vec<MetricPoint>,
}

/// 流关闭消息。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClosedMessage {
    /// 协议版本。
    pub schema_version: u8,
    /// 进程来源。
    pub source: Source,
    /// 流引用。
    pub stream: StreamReference,
    /// 流内数据报序列。
    pub sequence: u64,
    /// 发送时间。
    pub sent_at_unix_ms: u64,
    /// 关闭信息。
    pub closed: ClosedBody,
}

/// v1 三种消息。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "messageType")]
pub enum Message {
    /// 流打开。
    #[serde(rename = "performance.stream.opened")]
    StreamOpened(OpenedMessage),
    /// 指标快照。
    #[serde(rename = "performance.metrics.snapshot")]
    MetricsSnapshot(SnapshotMessage),
    /// 流关闭。
    #[serde(rename = "performance.stream.closed")]
    StreamClosed(ClosedMessage),
}

impl Message {
    /// 返回来源信息。
    pub const fn source(&self) -> &Source {
        match self {
            Self::StreamOpened(message) => &message.source,
            Self::MetricsSnapshot(message) => &message.source,
            Self::StreamClosed(message) => &message.source,
        }
    }

    /// 返回匿名流 ID。
    pub fn stream_id(&self) -> &str {
        match self {
            Self::StreamOpened(message) => &message.stream.id,
            Self::MetricsSnapshot(message) => &message.stream.id,
            Self::StreamClosed(message) => &message.stream.id,
        }
    }

    /// 返回流内 sequence。
    pub const fn sequence(&self) -> u64 {
        match self {
            Self::StreamOpened(message) => message.sequence,
            Self::MetricsSnapshot(message) => message.sequence,
            Self::StreamClosed(message) => message.sequence,
        }
    }
}

/// 数据报校验错误。
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// 数据报为空。
    #[error("empty datagram")]
    Empty,
    /// 数据报超过协议上限。
    #[error("datagram exceeds {MAX_DATAGRAM_BYTES} bytes")]
    DatagramTooLarge,
    /// JSON 无法解析。
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// 顶层或嵌套对象出现未知字段。
    #[error("unknown or missing field in {0}")]
    InvalidShape(&'static str),
    /// 不支持的协议版本。
    #[error("unsupported schema version")]
    UnsupportedVersion,
    /// 来源信息非法。
    #[error("invalid source")]
    InvalidSource,
    /// 流身份或参数非法。
    #[error("invalid stream")]
    InvalidStream,
    /// 快照分片信息非法。
    #[error("invalid snapshot")]
    InvalidSnapshot,
    /// 指标非法。
    #[error("invalid metric: {0}")]
    Metric(#[from] ValidationError),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageHeader {
    schema_version: u8,
}

/// 解析并严格校验一个完整 UDP 数据报。
pub fn decode_datagram(bytes: &[u8]) -> Result<Message, ProtocolError> {
    if bytes.is_empty() {
        return Err(ProtocolError::Empty);
    }
    if bytes.len() > MAX_DATAGRAM_BYTES {
        return Err(ProtocolError::DatagramTooLarge);
    }
    let header: MessageHeader = serde_json::from_slice(bytes)?;
    validate_version(header.schema_version)?;
    let message: Message = serde_json::from_slice(bytes)?;
    validate_message(&message)?;
    Ok(message)
}

/// 校验一个已构造的 v1 消息。
pub fn validate_message(message: &Message) -> Result<(), ProtocolError> {
    validate_source(message.source())?;
    match message {
        Message::StreamOpened(message) => {
            validate_version(message.schema_version)?;
            validate_stream(
                &message.stream.id,
                message.stream.domain,
                message.stream.kind,
                &message.stream.target,
                &message.stream.correlation,
            )?;
            if message.sequence != 0
                || !valid_wire_timestamp(message.sent_at_unix_ms)
                || !valid_wire_timestamp(message.stream.started_at_unix_ms)
                || message.stream.started_at_unix_ms > message.sent_at_unix_ms
            {
                return Err(ProtocolError::InvalidStream);
            }
            validate_parameters(message.stream.kind, &message.stream.parameters)?;
        }
        Message::MetricsSnapshot(message) => {
            validate_version(message.schema_version)?;
            validate_stream(
                &message.stream.id,
                message.stream.domain,
                message.stream.kind,
                &message.stream.target,
                &message.stream.correlation,
            )?;
            if message.sequence == 0
                || message.sequence > MAX_WIRE_INTEGER
                || !valid_wire_timestamp(message.sent_at_unix_ms)
                || message.part_count == 0
                || message.part_count > MAX_SNAPSHOT_PARTS
                || message.part_index >= message.part_count
                || !valid_batch_id(&message.batch_id, &message.stream.id)
                || !valid_wire_timestamp(message.window.started_at_unix_ms)
                || message.window.duration_ms == 0
                || message.window.duration_ms > MAX_WINDOW_DURATION_MS
                || message
                    .window
                    .started_at_unix_ms
                    .checked_add(message.window.duration_ms)
                    .is_none_or(|ended_at| ended_at > message.sent_at_unix_ms)
                || message.metrics.is_empty()
            {
                return Err(ProtocolError::InvalidSnapshot);
            }
            for metric in &message.metrics {
                validate_metric_for_domain(metric, message.stream.domain)?;
            }
        }
        Message::StreamClosed(message) => {
            validate_version(message.schema_version)?;
            validate_stream(
                &message.stream.id,
                message.stream.domain,
                message.stream.kind,
                &message.stream.target,
                &message.stream.correlation,
            )?;
            if message.sequence == 0
                || message.sequence > MAX_WIRE_INTEGER
                || !valid_wire_timestamp(message.sent_at_unix_ms)
                || !valid_wire_timestamp(message.closed.ended_at_unix_ms)
                || message.closed.ended_at_unix_ms > message.sent_at_unix_ms
            {
                return Err(ProtocolError::InvalidStream);
            }
        }
    }
    Ok(())
}

/// 校验并编码一个满足 v1 数据报上限的消息。
pub fn encode_datagram(message: &Message) -> Result<Vec<u8>, ProtocolError> {
    validate_message(message)?;
    let bytes = serde_json::to_vec(message)?;
    if bytes.len() > MAX_DATAGRAM_BYTES {
        return Err(ProtocolError::DatagramTooLarge);
    }
    Ok(bytes)
}

fn validate_version(version: u8) -> Result<(), ProtocolError> {
    if version == 1 {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion)
    }
}

fn validate_source(source: &Source) -> Result<(), ProtocolError> {
    if source.application != "fluxterm"
        || !valid_uuid_v4(&source.instance_id)
        || !valid_uuid_v4(&source.device.id)
        || source.device.name.as_deref().is_some_and(|name| {
            name.is_empty()
                || name.len() > 128
                || name.trim() != name
                || name.chars().any(char::is_control)
        })
        || !valid_label(&source.version, 32)
        || !valid_label(&source.platform, 32)
        || !valid_label(&source.arch, 32)
        || !valid_label(&source.build_profile, 32)
    {
        return Err(ProtocolError::InvalidSource);
    }
    Ok(())
}

fn validate_stream(
    id: &str,
    domain: PerformanceDomain,
    kind: StreamKind,
    target: &StreamTarget,
    correlation: &StreamCorrelation,
) -> Result<(), ProtocolError> {
    let transfer_valid = match domain {
        PerformanceDomain::Rdp => correlation.transfer_id.is_none(),
        PerformanceDomain::Sftp => correlation
            .transfer_id
            .as_deref()
            .is_some_and(valid_transfer_id),
    };
    if !valid_uuid_v4(id)
        || kind.domain() != domain
        || !valid_host(&target.host)
        || target.port == 0
        || !valid_uuid_v4(&correlation.session_id)
        || !transfer_valid
    {
        return Err(ProtocolError::InvalidStream);
    }
    Ok(())
}

fn valid_host(value: &str) -> bool {
    if value.is_empty() || value.len() > 253 || value.trim() != value {
        return false;
    }
    if value.parse::<IpAddr>().is_ok() {
        return true;
    }
    value.is_ascii()
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
        })
}

fn valid_transfer_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn validate_parameters(
    kind: StreamKind,
    parameters: &BTreeMap<String, StreamParameter>,
) -> Result<(), ProtocolError> {
    let valid = match kind {
        StreamKind::SftpUploadFile
        | StreamKind::SftpUploadDirectory
        | StreamKind::SftpUploadBatch
        | StreamKind::SftpDownloadFile
        | StreamKind::SftpDownloadDirectory => valid_sftp_parameters(parameters),
        StreamKind::RdpSession => valid_rdp_parameters(parameters),
    };
    valid.then_some(()).ok_or(ProtocolError::InvalidStream)
}

fn valid_label(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn valid_uuid_v4(value: &str) -> bool {
    let Ok(uuid) = Uuid::parse_str(value) else {
        return false;
    };
    uuid.get_variant() == Variant::RFC4122
        && uuid.get_version() == Some(Version::Random)
        && uuid.hyphenated().to_string() == value
}

const fn valid_wire_timestamp(value: u64) -> bool {
    value > 0 && value <= MAX_WIRE_INTEGER
}

fn valid_batch_id(value: &str, stream_id: &str) -> bool {
    let Some(suffix) = value
        .strip_prefix(stream_id)
        .and_then(|value| value.strip_prefix(':'))
    else {
        return false;
    };
    !suffix.is_empty()
        && suffix.bytes().all(|byte| byte.is_ascii_digit())
        && suffix
            .parse::<u64>()
            .is_ok_and(|value| value <= MAX_WIRE_INTEGER)
}

fn valid_sftp_parameters(parameters: &BTreeMap<String, StreamParameter>) -> bool {
    parameters.len() == 3
        && unsigned_parameter(parameters, "chunkSizeBytes")
            .is_some_and(|value| (1..=64 * 1024 * 1024).contains(&value))
        && unsigned_parameter(parameters, "requestWindow")
            .is_some_and(|value| (1..=1024).contains(&value))
        && unsigned_parameter(parameters, "workerCount")
            .is_some_and(|value| (1..=1024).contains(&value))
}

fn valid_rdp_parameters(parameters: &BTreeMap<String, StreamParameter>) -> bool {
    const BOOL_PARAMETERS: &[&str] = &[
        "wallpaper",
        "fullWindowDrag",
        "menuAnimations",
        "theming",
        "cursorShadow",
        "cursorSettings",
        "fontSmoothing",
        "desktopComposition",
    ];
    parameters.len() == 10
        && unsigned_parameter(parameters, "width").is_some_and(|value| (1..=16384).contains(&value))
        && unsigned_parameter(parameters, "height")
            .is_some_and(|value| (1..=16384).contains(&value))
        && BOOL_PARAMETERS
            .iter()
            .all(|name| bool_parameter(parameters, name).is_some())
}

fn unsigned_parameter(parameters: &BTreeMap<String, StreamParameter>, name: &str) -> Option<u64> {
    match parameters.get(name) {
        Some(StreamParameter::Unsigned(value)) => Some(*value),
        _ => None,
    }
}

fn bool_parameter(parameters: &BTreeMap<String, StreamParameter>, name: &str) -> Option<bool> {
    match parameters.get(name) {
        Some(StreamParameter::Bool(value)) => Some(*value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    const INSTANCE_ID: &str = "7d28b773-9bea-47a6-93a1-b52da63b74fa";
    const STREAM_ID: &str = "dc8df28e-a42d-4b9e-87e3-c38f89caa0d4";

    fn snapshot() -> Value {
        serde_json::json!({
            "schemaVersion": 1,
            "messageType": "performance.metrics.snapshot",
            "source": {
                "application": "fluxterm",
                "version": "0.8.0",
                "instanceId": INSTANCE_ID,
                "device": {
                    "id": "27ec5c1f-73d8-45d6-a3dc-6242203fc777",
                    "name": "WORKSTATION-01"
                },
                "platform": "windows",
                "arch": "x86_64",
                "buildProfile": "debug"
            },
            "stream": {
                "id": STREAM_ID,
                "domain": "rdp",
                "kind": "rdpSession",
                "target": {
                    "host": "rdp.internal",
                    "port": 3389
                },
                "correlation": {
                    "sessionId": "31a0ae31-4116-4909-95be-0b81c1ab2ad9"
                }
            },
            "sequence": 42,
            "batchId": format!("{STREAM_ID}:17"),
            "partIndex": 0,
            "partCount": 1,
            "sentAtUnixMs": 1780000000000_u64,
            "window": {
                "startedAtUnixMs": 1779999999000_u64,
                "durationMs": 1000
            },
            "metrics": [{
                "name": "fluxterm.rdp.renderer.fps",
                "kind": "gauge",
                "unit": "{frame}/s",
                "value": 60,
                "attributes": {
                    "rendererMode": "worker",
                    "visibility": "visible",
                    "resolutionClass": "fullHd"
                }
            }]
        })
    }

    fn opened() -> Value {
        serde_json::json!({
            "schemaVersion": 1,
            "messageType": "performance.stream.opened",
            "source": snapshot()["source"].clone(),
            "stream": {
                "id": STREAM_ID,
                "domain": "rdp",
                "kind": "rdpSession",
                "startedAtUnixMs": 1779999998000_u64,
                "parameters": {
                    "width": 1920,
                    "height": 1080,
                    "wallpaper": false,
                    "fullWindowDrag": false,
                    "menuAnimations": false,
                    "theming": true,
                    "cursorShadow": false,
                    "cursorSettings": true,
                    "fontSmoothing": true,
                    "desktopComposition": true
                },
                "target": {
                    "host": "rdp.internal",
                    "port": 3389
                },
                "correlation": {
                    "sessionId": "31a0ae31-4116-4909-95be-0b81c1ab2ad9"
                }
            },
            "sequence": 0,
            "sentAtUnixMs": 1779999999000_u64
        })
    }

    fn sftp_opened(kind: &str) -> Value {
        let mut value = opened();
        value["stream"]["domain"] = Value::String("sftp".into());
        value["stream"]["kind"] = Value::String(kind.into());
        value["stream"]["parameters"] = serde_json::json!({
            "chunkSizeBytes": 262144,
            "requestWindow": 8,
            "workerCount": 1
        });
        value["stream"]["target"]["port"] = Value::from(22);
        value["stream"]["correlation"]["transferId"] = Value::String("transfer-1".into());
        value
    }

    #[test]
    fn decodes_sender_compatible_snapshot() {
        let bytes = serde_json::to_vec(&snapshot()).expect("fixture");
        let message = decode_datagram(&bytes).expect("valid snapshot");
        assert_eq!(message.stream_id(), STREAM_ID);
        assert_eq!(message.sequence(), 42);
    }

    #[test]
    fn rejects_unknown_field_and_oversized_datagram() {
        let mut value = snapshot();
        value
            .as_object_mut()
            .expect("object")
            .insert("sessionId".into(), Value::String("secret".into()));
        let bytes = serde_json::to_vec(&value).expect("fixture");
        assert!(matches!(
            decode_datagram(&bytes),
            Err(ProtocolError::Json(_))
        ));
        assert!(matches!(
            decode_datagram(&vec![b'x'; MAX_DATAGRAM_BYTES + 1]),
            Err(ProtocolError::DatagramTooLarge)
        ));
    }

    #[test]
    fn validates_version_before_message_shape() {
        let value = serde_json::json!({
            "schemaVersion": 2,
            "futureField": true
        });
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::UnsupportedVersion)
        ));
    }

    #[test]
    fn encodes_only_valid_and_bounded_messages() {
        let message: Message = serde_json::from_value(snapshot()).expect("message");
        let bytes = encode_datagram(&message).expect("encoded");
        assert_eq!(decode_datagram(&bytes).expect("round trip"), message);

        let mut oversized = message;
        let Message::MetricsSnapshot(snapshot) = &mut oversized else {
            unreachable!()
        };
        snapshot.metrics.extend(snapshot.metrics.clone());
        while serde_json::to_vec(&oversized).expect("JSON").len() <= MAX_DATAGRAM_BYTES {
            let Message::MetricsSnapshot(snapshot) = &mut oversized else {
                unreachable!()
            };
            snapshot.metrics.extend(snapshot.metrics.clone());
        }
        assert!(matches!(
            encode_datagram(&oversized),
            Err(ProtocolError::DatagramTooLarge)
        ));
    }

    #[test]
    fn rejects_unknown_metric_and_invalid_histogram() {
        let mut value = snapshot();
        value["metrics"][0]["name"] = Value::String("fluxterm.secret".into());
        let bytes = serde_json::to_vec(&value).expect("fixture");
        assert!(matches!(
            decode_datagram(&bytes),
            Err(ProtocolError::Metric(ValidationError::UnknownMetric))
        ));
    }

    #[test]
    fn validates_device_target_and_business_correlation() {
        let mut value = snapshot();
        value["source"]["device"]["id"] = Value::String("invalid".into());
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::InvalidSource)
        ));

        let mut value = snapshot();
        value["stream"]["target"]["host"] = Value::String("https://rdp.internal/path".into());
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::InvalidStream)
        ));

        let mut value = snapshot();
        value["stream"]["target"]["host"] = Value::String("fd00::1".into());
        assert!(decode_datagram(&serde_json::to_vec(&value).expect("fixture")).is_ok());

        let mut value = snapshot();
        value["stream"]["id"] = Value::String(Uuid::nil().to_string());
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::InvalidStream)
        ));

        let mut value = snapshot();
        value["stream"]["id"] = Value::String(STREAM_ID.to_uppercase());
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::InvalidStream)
        ));
    }

    #[test]
    fn sftp_requires_transfer_id_and_rdp_rejects_it() {
        let mut rdp = snapshot();
        rdp["stream"]["correlation"]["transferId"] = Value::String("sftp-1".into());
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&rdp).expect("fixture")),
            Err(ProtocolError::InvalidStream)
        ));

        let mut sftp = snapshot();
        sftp["stream"]["domain"] = Value::String("sftp".into());
        sftp["stream"]["kind"] = Value::String("sftpUploadFile".into());
        sftp["stream"]["target"]["port"] = Value::from(22);
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&sftp).expect("fixture")),
            Err(ProtocolError::InvalidStream)
        ));
        sftp["stream"]["correlation"]["transferId"] = Value::String("sftp-1780000000000".into());
        sftp["metrics"] = serde_json::json!([{
            "name": "fluxterm.sftp.transfer.throughput",
            "kind": "gauge",
            "unit": "By/s",
            "value": 1024,
            "attributes": {}
        }]);
        assert!(decode_datagram(&serde_json::to_vec(&sftp).expect("fixture")).is_ok());
    }

    #[test]
    fn validates_opened_parameters_and_timeline() {
        assert!(decode_datagram(&serde_json::to_vec(&opened()).expect("fixture")).is_ok());

        let mut value = opened();
        value["stream"]["parameters"]["width"] = Value::Bool(true);
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::InvalidStream)
        ));

        let mut value = opened();
        value["sequence"] = Value::from(1);
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::InvalidStream)
        ));

        let mut value = opened();
        value["stream"]["startedAtUnixMs"] = Value::from(1780000000000_u64);
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::InvalidStream)
        ));
    }

    #[test]
    fn validates_clean_sftp_stream_kinds_and_parameters() {
        for kind in [
            "sftpUploadFile",
            "sftpUploadDirectory",
            "sftpUploadBatch",
            "sftpDownloadFile",
            "sftpDownloadDirectory",
        ] {
            let value = sftp_opened(kind);
            assert!(
                decode_datagram(&serde_json::to_vec(&value).expect("fixture")).is_ok(),
                "expected {kind} to be accepted"
            );
        }

        for legacy_kind in ["sftpUploadSingle", "sftpDownloadSingle"] {
            let value = sftp_opened(legacy_kind);
            assert!(matches!(
                decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
                Err(ProtocolError::Json(_))
            ));
        }

        for redundant_parameter in ["direction", "mode"] {
            let mut value = sftp_opened("sftpUploadFile");
            value["stream"]["parameters"][redundant_parameter] = Value::String("upload".into());
            assert!(matches!(
                decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
                Err(ProtocolError::InvalidStream)
            ));
        }
    }

    #[test]
    fn validates_snapshot_domain_fragments_and_timeline() {
        let mut value = snapshot();
        value["stream"]["domain"] = Value::String("sftp".into());
        value["stream"]["kind"] = Value::String("sftpUploadFile".into());
        value["stream"]["target"]["port"] = Value::from(22);
        value["stream"]["correlation"]["transferId"] = Value::String("transfer-1".into());
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::Metric(ValidationError::DomainMismatch))
        ));

        let mut value = snapshot();
        value["partCount"] = Value::from(MAX_SNAPSHOT_PARTS + 1);
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::InvalidSnapshot)
        ));

        let mut value = snapshot();
        value["batchId"] = Value::String(format!("{STREAM_ID}:not-a-number"));
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::InvalidSnapshot)
        ));

        let mut value = snapshot();
        value["window"]["durationMs"] = Value::from(MAX_WINDOW_DURATION_MS + 1);
        assert!(matches!(
            decode_datagram(&serde_json::to_vec(&value).expect("fixture")),
            Err(ProtocolError::InvalidSnapshot)
        ));
    }
}
