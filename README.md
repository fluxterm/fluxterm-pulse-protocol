# FluxTerm Pulse Protocol

`fluxterm-pulse-protocol` 是 FluxTerm 性能遥测发送端与 Pulse Server 共享的 Rust 协议库。它定义版本化 UDP JSON 线协议、SFTP/RDP 指标目录、数据模型和严格校验规则，确保两端对报文结构和指标语义使用同一事实来源。

## 项目结构

```text
src/
  lib.rs
  v1/
    mod.rs        v1 公共导出
    message.rs    消息模型与数据报校验
    metric.rs     指标目录、类型与指标校验
```

## 开发与验证

```powershell
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```
