//! FluxTerm Pulse 性能遥测协议。
//!
//! 本 crate 只定义版本化线协议、指标目录和接收端校验，不包含 UDP、HTTP、
//! 数据库或业务日志实现。

pub mod v1;

/// 当前接收端支持的最新协议版本。
pub const LATEST_SCHEMA_VERSION: u8 = 1;
