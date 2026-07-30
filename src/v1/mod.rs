//! FluxTerm 性能遥测 UDP JSON v1。

mod message;
mod metric;

pub use message::{
    ClosedBody, ClosedMessage, DeviceIdentity, MAX_DATAGRAM_BYTES, MAX_SNAPSHOT_PARTS,
    MAX_WINDOW_DURATION_MS, MAX_WIRE_INTEGER, Message, MetricWindow, OpenedMessage, ProtocolError,
    SnapshotMessage, Source, StreamCorrelation, StreamDescriptor, StreamReference, StreamTarget,
    decode_datagram, encode_datagram, validate_message,
};
pub use metric::{
    HistogramValue, METRIC_CATALOG, MetricDefinition, MetricKind, MetricPoint, MetricUnit,
    MetricValue, PerformanceDomain, StreamKind, StreamOutcome, StreamParameter, ValidationError,
    definition, validate_metric, validate_metric_for_domain,
};
