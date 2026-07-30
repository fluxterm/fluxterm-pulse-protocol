# Changelog

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，并遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [0.1.0] - 2026-07-30

### Added

- **v1 遥测协议**：定义性能流打开、指标快照和性能流关闭三类 UDP JSON 生命周期消息。
- **SFTP 与 RDP 模型**：提供文件上传、目录上传、批量上传、文件下载、目录下载和 RDP 会话六种明确的性能流类型。
- **指标目录**：集中定义 SFTP 传输和 RDP 渲染、运行时指标的名称、类型、单位、属性及直方图边界。
- **严格协议校验**：校验协议版本、UUID、设备与目标连接身份、业务关联、流参数、指标语义、快照分片和时间窗口。
- **有界数据报编码**：限制单个 UTF-8 JSON 数据报不超过 1200 字节，并拒绝未知字段、非法数值和不完整直方图。
