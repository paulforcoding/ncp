# ncp 知识库索引

> **首次使用？** 先读 00-overview.md — 了解 ncp 是什么、解决什么问题。

## 按场景检索

### 我要执行 copy
→ 10-cli/copy.md
→ 如果目标为 ncp:// → 10-cli/serve.md + 20-concepts/url-schemes.md
→ 如果涉及 OSS/COS/OBS → 20-concepts/profiles.md
→ 如果要了解进度状态 → 20-concepts/log.md + 20-concepts/progress-model.md

### 我要执行 cksum
→ 10-cli/cksum.md
→ 如果目标为 ncp:// → 不支持，引导用户用 --task 模式
→ 如果涉及 OSS → 算法必须为 md5（见 10-cli/cksum.md）

### 我要执行 resume
→ 10-cli/resume.md
→ 如果目标为 ncp:// → 需先重启 serve（见 10-cli/serve.md）

### 我要启动 serve
→ 10-cli/serve.md
→ serve 只有 --listen 和 --serve-temp-dir 两个 flag

### 我要管理 task
→ 10-cli/task-config.md

## 按概念检索

| 概念 | 文件 |
|------|------|
| 路径语义（basename 自动拼接、多源规则、ncp:// 特殊行为） | 20-concepts/path-semantics.md |
| URL 格式（ncp:// oss:// cos:// obs://） | 20-concepts/url-schemes.md |
| Cloud Profile（配置、${env:}、0600） | 20-concepts/profiles.md |
| 进度状态机（[CopyStatus][CksumStatus]） | 20-concepts/progress-model.md |
| 三条信息通道（stderr/stdout、ProgramLog、FileLog） | 20-concepts/log.md |
| 跳过策略（mtime/size/ETag、skipped 含义） | 20-concepts/skip-logic.md |
| 退出码（0/1/2 的含义） | 20-concepts/exit-codes.md |

## 按错误信息检索

| 错误信息关键词 | 读这个 |
|--------------|--------|
| "connection refused" / "dial tcp" | 10-cli/serve.md |
| "profile ... not defined" / "requires a profile" | 20-concepts/profiles.md |
| "Provider ... does not match" | 20-concepts/profiles.md |
| "credential file permissions" / "0600" | 20-concepts/profiles.md |
| "embedding password in URL" | 20-concepts/url-schemes.md |
| "unknown flag" (ncp serve) | 10-cli/serve.md |
| "checksum mismatch" | 10-cli/cksum.md |
| "Direct IO" / "SyncWrites" / "mutually exclusive" | 10-cli/copy.md |
