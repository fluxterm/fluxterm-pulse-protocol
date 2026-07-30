//! 指标目录及指标值校验。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 性能遥测业务域。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PerformanceDomain {
    /// SFTP 文件传输。
    Sftp,
    /// RDP 会话与渲染。
    Rdp,
}

/// 匿名性能流类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StreamKind {
    /// 文件上传。
    SftpUploadFile,
    /// 目录上传。
    SftpUploadDirectory,
    /// 批量上传。
    SftpUploadBatch,
    /// 文件下载。
    SftpDownloadFile,
    /// 目录下载。
    SftpDownloadDirectory,
    /// RDP 会话。
    RdpSession,
}

impl StreamKind {
    /// 返回该流类型所属的业务域。
    pub const fn domain(self) -> PerformanceDomain {
        match self {
            Self::SftpUploadFile
            | Self::SftpUploadDirectory
            | Self::SftpUploadBatch
            | Self::SftpDownloadFile
            | Self::SftpDownloadDirectory => PerformanceDomain::Sftp,
            Self::RdpSession => PerformanceDomain::Rdp,
        }
    }
}

/// 流打开报文允许携带的低基数参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StreamParameter {
    /// 布尔参数。
    Bool(bool),
    /// 无符号整数参数。
    Unsigned(u64),
    /// 受控字符串参数。
    Text(String),
}

/// 流关闭结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamOutcome {
    /// 成功。
    Succeeded,
    /// 失败。
    Failed,
    /// 已取消。
    Cancelled,
    /// 部分成功。
    Partial,
    /// 会话断开。
    Disconnected,
}

/// 指标聚合类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MetricKind {
    /// 窗口最后值。
    Gauge,
    /// 窗口增量。
    CounterDelta,
    /// 固定桶直方图。
    Histogram,
}

/// 指标单位。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricUnit {
    /// 字节。
    #[serde(rename = "By")]
    Byte,
    /// 字节每秒。
    #[serde(rename = "By/s")]
    BytePerSecond,
    /// 毫秒。
    #[serde(rename = "ms")]
    Millisecond,
    /// 微秒。
    #[serde(rename = "us")]
    Microsecond,
    /// 无量纲计数。
    #[serde(rename = "1")]
    Count,
    /// 请求数。
    #[serde(rename = "{request}")]
    Request,
    /// 项目数。
    #[serde(rename = "{item}")]
    Item,
    /// 数据块数。
    #[serde(rename = "{chunk}")]
    Chunk,
    /// 帧数。
    #[serde(rename = "{frame}")]
    Frame,
    /// 帧每秒。
    #[serde(rename = "{frame}/s")]
    FramePerSecond,
    /// 像素。
    #[serde(rename = "px")]
    Pixel,
}

/// 固定桶直方图值。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HistogramValue {
    /// 样本数。
    pub count: u64,
    /// 样本和。
    pub sum: f64,
    /// 最小样本。
    pub min: f64,
    /// 最大样本。
    pub max: f64,
    /// 有限桶上界。
    pub bounds: Vec<f64>,
    /// 各有限桶及最终溢出桶的样本数。
    pub bucket_counts: Vec<u64>,
}

/// 指标聚合值。
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    /// Gauge 或 counter delta。
    Scalar(f64),
    /// 固定桶直方图。
    Histogram(HistogramValue),
}

/// 完整指标点。
#[derive(Debug, Clone, PartialEq)]
pub struct MetricPoint {
    /// 指标名。
    pub name: String,
    /// 聚合类型。
    pub kind: MetricKind,
    /// 单位。
    pub unit: MetricUnit,
    /// 聚合值。
    pub value: MetricValue,
    /// 低基数属性。
    pub attributes: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScalarMetricPoint {
    name: String,
    kind: MetricKind,
    unit: MetricUnit,
    value: f64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    attributes: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HistogramMetricPoint {
    name: String,
    kind: MetricKind,
    unit: MetricUnit,
    histogram: HistogramValue,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    attributes: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum WireMetricPoint {
    Scalar(ScalarMetricPoint),
    Histogram(HistogramMetricPoint),
}

impl Serialize for MetricPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.value {
            MetricValue::Scalar(value) => WireMetricPoint::Scalar(ScalarMetricPoint {
                name: self.name.clone(),
                kind: self.kind,
                unit: self.unit,
                value: *value,
                attributes: self.attributes.clone(),
            })
            .serialize(serializer),
            MetricValue::Histogram(histogram) => WireMetricPoint::Histogram(HistogramMetricPoint {
                name: self.name.clone(),
                kind: self.kind,
                unit: self.unit,
                histogram: histogram.clone(),
                attributes: self.attributes.clone(),
            })
            .serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for MetricPoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = WireMetricPoint::deserialize(deserializer)?;
        Ok(match wire {
            WireMetricPoint::Scalar(point) => Self {
                name: point.name,
                kind: point.kind,
                unit: point.unit,
                value: MetricValue::Scalar(point.value),
                attributes: point.attributes,
            },
            WireMetricPoint::Histogram(point) => Self {
                name: point.name,
                kind: point.kind,
                unit: point.unit,
                value: MetricValue::Histogram(point.histogram),
                attributes: point.attributes,
            },
        })
    }
}

/// 指标目录记录。
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricDefinition {
    /// 指标名。
    pub name: &'static str,
    /// 聚合类型。
    pub kind: MetricKind,
    /// 单位。
    pub unit: MetricUnit,
    /// 允许属性。
    pub allowed_attributes: &'static [&'static str],
    /// 直方图桶边界。
    pub histogram_bounds: &'static [f64],
}

impl MetricDefinition {
    /// 返回指标所属的业务域。
    pub fn domain(&self) -> PerformanceDomain {
        if self.name.starts_with("fluxterm.sftp.") {
            PerformanceDomain::Sftp
        } else {
            PerformanceDomain::Rdp
        }
    }
}

const FRAME_INTERVAL_BOUNDS: &[f64] = &[
    4.0, 8.0, 12.0, 16.67, 25.0, 33.33, 50.0, 100.0, 250.0, 1000.0,
];
const LATENCY_BOUNDS: &[f64] = &[
    0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 33.0, 100.0, 500.0, 1000.0,
];
const TRANSFER_DURATION_BOUNDS: &[f64] = &[
    100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0, 60000.0,
];
const RDP_ATTRIBUTES: &[&str] = &["rendererMode", "visibility", "resolutionClass"];

macro_rules! metric {
    ($name:literal, $kind:ident, $unit:ident) => {
        MetricDefinition {
            name: $name,
            kind: MetricKind::$kind,
            unit: MetricUnit::$unit,
            allowed_attributes: &[],
            histogram_bounds: &[],
        }
    };
    ($name:literal, $kind:ident, $unit:ident, $attributes:expr, $bounds:expr) => {
        MetricDefinition {
            name: $name,
            kind: MetricKind::$kind,
            unit: MetricUnit::$unit,
            allowed_attributes: $attributes,
            histogram_bounds: $bounds,
        }
    };
}

/// v1 允许的全部指标。
pub static METRIC_CATALOG: &[MetricDefinition] = &[
    metric!("fluxterm.sftp.transfer.bytes", CounterDelta, Byte),
    metric!("fluxterm.sftp.transfer.throughput", Gauge, BytePerSecond),
    metric!("fluxterm.sftp.request.count", CounterDelta, Request),
    metric!(
        "fluxterm.sftp.request.duration",
        Histogram,
        Millisecond,
        &[],
        LATENCY_BOUNDS
    ),
    metric!("fluxterm.sftp.request.in_flight.max", Gauge, Request),
    metric!("fluxterm.sftp.download.pending_chunks.max", Gauge, Chunk),
    metric!("fluxterm.sftp.item.completed", CounterDelta, Item),
    metric!("fluxterm.sftp.item.failed", CounterDelta, Item),
    metric!(
        "fluxterm.sftp.scan.duration",
        Histogram,
        Millisecond,
        &[],
        TRANSFER_DURATION_BOUNDS
    ),
    metric!(
        "fluxterm.sftp.transfer.duration",
        Histogram,
        Millisecond,
        &[],
        TRANSFER_DURATION_BOUNDS
    ),
    metric!("fluxterm.sftp.transfer.size", Gauge, Byte),
    metric!(
        "fluxterm.sftp.transfer.average_throughput",
        Gauge,
        BytePerSecond
    ),
    metric!("fluxterm.rdp.runtime.update_cycles", CounterDelta, Count),
    metric!("fluxterm.rdp.runtime.raw_rects", CounterDelta, Count),
    metric!("fluxterm.rdp.runtime.merged_rects", CounterDelta, Count),
    metric!("fluxterm.rdp.runtime.received_bytes", CounterDelta, Byte),
    metric!("fluxterm.rdp.runtime.encoded_bytes", CounterDelta, Byte),
    metric!("fluxterm.rdp.runtime.sent_pixels", CounterDelta, Pixel),
    metric!("fluxterm.rdp.runtime.messages", CounterDelta, Count),
    metric!("fluxterm.rdp.runtime.resize_requests", CounterDelta, Count),
    metric!("fluxterm.rdp.runtime.timeout_flushes", CounterDelta, Count),
    metric!("fluxterm.rdp.runtime.pending_rects.max", Gauge, Count),
    metric!(
        "fluxterm.rdp.runtime.flush_interval.max",
        Gauge,
        Millisecond
    ),
    metric!(
        "fluxterm.rdp.runtime.read_pdu_cpu",
        CounterDelta,
        Microsecond
    ),
    metric!("fluxterm.rdp.runtime.decode_cpu", CounterDelta, Microsecond),
    metric!("fluxterm.rdp.runtime.copy_cpu", CounterDelta, Microsecond),
    metric!("fluxterm.rdp.runtime.encode_cpu", CounterDelta, Microsecond),
    metric!(
        "fluxterm.rdp.runtime.bridge_send_cpu",
        CounterDelta,
        Microsecond
    ),
    metric!(
        "fluxterm.rdp.renderer.fps",
        Gauge,
        FramePerSecond,
        RDP_ATTRIBUTES,
        &[]
    ),
    metric!(
        "fluxterm.rdp.renderer.received_frames",
        CounterDelta,
        Frame,
        RDP_ATTRIBUTES,
        &[]
    ),
    metric!(
        "fluxterm.rdp.renderer.presented_frames",
        CounterDelta,
        Frame,
        RDP_ATTRIBUTES,
        &[]
    ),
    metric!(
        "fluxterm.rdp.renderer.dropped_frames",
        CounterDelta,
        Frame,
        RDP_ATTRIBUTES,
        &[]
    ),
    metric!(
        "fluxterm.rdp.renderer.frame_interval",
        Histogram,
        Millisecond,
        RDP_ATTRIBUTES,
        FRAME_INTERVAL_BOUNDS
    ),
    metric!(
        "fluxterm.rdp.renderer.render_duration",
        Histogram,
        Millisecond,
        RDP_ATTRIBUTES,
        LATENCY_BOUNDS
    ),
    metric!(
        "fluxterm.rdp.renderer.queue_depth.max",
        Gauge,
        Frame,
        RDP_ATTRIBUTES,
        &[]
    ),
    metric!(
        "fluxterm.rdp.renderer.width",
        Gauge,
        Pixel,
        RDP_ATTRIBUTES,
        &[]
    ),
    metric!(
        "fluxterm.rdp.renderer.height",
        Gauge,
        Pixel,
        RDP_ATTRIBUTES,
        &[]
    ),
];

/// 指标校验错误。
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidationError {
    /// 指标未登记。
    #[error("unknown metric")]
    UnknownMetric,
    /// 聚合类型不匹配。
    #[error("metric kind mismatch")]
    KindMismatch,
    /// 单位不匹配。
    #[error("metric unit mismatch")]
    UnitMismatch,
    /// 指标所属业务域与流不一致。
    #[error("metric domain mismatch")]
    DomainMismatch,
    /// 数值非法。
    #[error("invalid metric value")]
    InvalidValue,
    /// 属性非法。
    #[error("invalid metric attribute")]
    InvalidAttribute,
    /// 直方图结构非法。
    #[error("invalid histogram")]
    InvalidHistogram,
}

/// 查询一个 v1 指标定义。
pub fn definition(name: &str) -> Option<&'static MetricDefinition> {
    METRIC_CATALOG.iter().find(|item| item.name == name)
}

/// 严格校验一个指标点。
pub fn validate_metric(point: &MetricPoint) -> Result<(), ValidationError> {
    let definition = definition(&point.name).ok_or(ValidationError::UnknownMetric)?;
    if point.kind != definition.kind {
        return Err(ValidationError::KindMismatch);
    }
    if point.unit != definition.unit {
        return Err(ValidationError::UnitMismatch);
    }
    for (name, value) in &point.attributes {
        if !definition.allowed_attributes.contains(&name.as_str())
            || !valid_attribute_value(name, value)
        {
            return Err(ValidationError::InvalidAttribute);
        }
    }
    match &point.value {
        MetricValue::Scalar(value) => {
            if !value.is_finite() || *value < 0.0 || point.kind == MetricKind::Histogram {
                return Err(ValidationError::InvalidValue);
            }
        }
        MetricValue::Histogram(histogram) => {
            if point.kind != MetricKind::Histogram
                || histogram.count == 0
                || !histogram.sum.is_finite()
                || !histogram.min.is_finite()
                || !histogram.max.is_finite()
                || histogram.min < 0.0
                || histogram.min > histogram.max
                || histogram.bounds != definition.histogram_bounds
                || histogram.bucket_counts.len() != histogram.bounds.len() + 1
                || !valid_histogram_summary(histogram)
            {
                return Err(ValidationError::InvalidHistogram);
            }
        }
    }
    Ok(())
}

/// 严格校验指标点及其所属业务域。
pub fn validate_metric_for_domain(
    point: &MetricPoint,
    domain: PerformanceDomain,
) -> Result<(), ValidationError> {
    let definition = definition(&point.name).ok_or(ValidationError::UnknownMetric)?;
    if definition.domain() != domain {
        return Err(ValidationError::DomainMismatch);
    }
    validate_metric(point)
}

fn valid_attribute_value(name: &str, value: &str) -> bool {
    match name {
        "rendererMode" => matches!(value, "worker" | "main-thread" | "none"),
        "visibility" => matches!(value, "visible" | "hidden"),
        "resolutionClass" => matches!(value, "hd" | "fullHd" | "quadHd" | "ultraHd"),
        _ => false,
    }
}

fn valid_histogram_summary(histogram: &HistogramValue) -> bool {
    let Some(bucket_sum) = histogram
        .bucket_counts
        .iter()
        .try_fold(0_u64, |sum, count| sum.checked_add(*count))
    else {
        return false;
    };
    if bucket_sum != histogram.count {
        return false;
    }

    let count = histogram.count as f64;
    let minimum_sum = count * histogram.min;
    let maximum_sum = count * histogram.max;
    if !minimum_sum.is_finite() || !maximum_sum.is_finite() {
        return false;
    }
    let tolerance = maximum_sum.abs().max(histogram.sum.abs()).max(1.0) * 1e-9;
    if histogram.sum + tolerance < minimum_sum || histogram.sum - tolerance > maximum_sum {
        return false;
    }

    let Some(first_bucket) = histogram.bucket_counts.iter().position(|count| *count > 0) else {
        return false;
    };
    let Some(last_bucket) = histogram.bucket_counts.iter().rposition(|count| *count > 0) else {
        return false;
    };
    value_belongs_to_bucket(histogram.min, first_bucket, &histogram.bounds)
        && value_belongs_to_bucket(histogram.max, last_bucket, &histogram.bounds)
}

fn value_belongs_to_bucket(value: f64, index: usize, bounds: &[f64]) -> bool {
    let above_lower_bound = index == 0 || value > bounds[index - 1];
    let below_upper_bound = index == bounds.len() || value <= bounds[index];
    above_lower_bound && below_upper_bound
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_matches_sender_metric_count() {
        assert_eq!(METRIC_CATALOG.len(), 37);
    }

    #[test]
    fn rejects_sensitive_attribute_and_non_finite_value() {
        let point = MetricPoint {
            name: "fluxterm.rdp.renderer.fps".into(),
            kind: MetricKind::Gauge,
            unit: MetricUnit::FramePerSecond,
            value: MetricValue::Scalar(60.0),
            attributes: BTreeMap::from([("sessionId".into(), "secret".into())]),
        };
        assert_eq!(
            validate_metric(&point),
            Err(ValidationError::InvalidAttribute)
        );

        let point = MetricPoint {
            attributes: BTreeMap::new(),
            value: MetricValue::Scalar(f64::NAN),
            ..point
        };
        assert_eq!(validate_metric(&point), Err(ValidationError::InvalidValue));
    }

    #[test]
    fn rejects_histogram_bucket_overflow_and_inconsistent_summary() {
        let definition =
            definition("fluxterm.rdp.renderer.frame_interval").expect("registered metric");
        let histogram = HistogramValue {
            count: 1,
            sum: 5.0,
            min: 5.0,
            max: 5.0,
            bounds: definition.histogram_bounds.to_vec(),
            bucket_counts: [
                vec![u64::MAX, 2],
                vec![0; definition.histogram_bounds.len() - 1],
            ]
            .concat(),
        };
        let point = MetricPoint {
            name: definition.name.into(),
            kind: definition.kind,
            unit: definition.unit,
            value: MetricValue::Histogram(histogram),
            attributes: BTreeMap::new(),
        };
        assert_eq!(
            validate_metric(&point),
            Err(ValidationError::InvalidHistogram)
        );

        let mut histogram = match &point.value {
            MetricValue::Histogram(histogram) => histogram.clone(),
            MetricValue::Scalar(_) => unreachable!(),
        };
        histogram.count = 1;
        histogram.bucket_counts.fill(0);
        histogram.bucket_counts[1] = 1;
        histogram.sum = 100.0;
        let point = MetricPoint {
            value: MetricValue::Histogram(histogram),
            ..point
        };
        assert_eq!(
            validate_metric(&point),
            Err(ValidationError::InvalidHistogram)
        );
    }

    #[test]
    fn validates_metric_domain_and_attribute_enums() {
        let mut point = MetricPoint {
            name: "fluxterm.rdp.renderer.fps".into(),
            kind: MetricKind::Gauge,
            unit: MetricUnit::FramePerSecond,
            value: MetricValue::Scalar(60.0),
            attributes: BTreeMap::from([("rendererMode".into(), "worker".into())]),
        };
        assert!(validate_metric_for_domain(&point, PerformanceDomain::Rdp).is_ok());
        assert_eq!(
            validate_metric_for_domain(&point, PerformanceDomain::Sftp),
            Err(ValidationError::DomainMismatch)
        );
        point
            .attributes
            .insert("rendererMode".into(), "session-123".into());
        assert_eq!(
            validate_metric(&point),
            Err(ValidationError::InvalidAttribute)
        );
    }
}
