# ncp 知识库索引

> **首次使用？** 先读 00-overview.md — 了解 ncp 是什么、解决什么问题。

## 按场景检索

### 我要执行 copy
→ 10-cli/copy.md [TODO]
→ 如果目标为 ncp:// → 10-cli/serve.md [TODO] + 20-concepts/url-schemes.md [TODO]
→ 如果涉及 OSS/COS/OBS → 20-concepts/profiles.md [TODO]
→ 如果要了解进度状态 → 20-concepts/log.md [TODO] + 20-concepts/progress-model.md [TODO]

### 我要执行 cksum
→ 10-cli/cksum.md [TODO]
→ 如果目标为 ncp:// → 不支持，引导用户用 --task 模式
→ 如果涉及 OSS → 算法必须为 md5（见 10-cli/cksum.md [TODO]）

### 我要执行 resume
→ 10-cli/resume.md [TODO]
→ 如果目标为 ncp:// → 需先重启 serve（见 10-cli/serve.md [TODO]）

### 我要启动 serve
→ 10-cli/serve.md [TODO]
→ serve 只有 --listen 和 --serve-temp-dir 两个 flag

### 我要管理 task
→ 10-cli/task-config.md [TODO]

## 按概念检索

| 概念 | 文件 |
|------|------|
| 路径语义（basename 自动拼接、多源规则） | 20-concepts/path-semantics.md [TODO] |
| URL 格式（ncp:// oss:// cos:// obs://） | 20-concepts/url-schemes.md [TODO] |
| Cloud Profile（配置、${env:}、0600） | 20-concepts/profiles.md [TODO] |
| 进度状态机（[CopyStatus][CksumStatus]） | 20-concepts/progress-model.md [TODO] |
| 三条信息通道（stderr/stdout、ProgramLog、FileLog） | 20-concepts/log.md |
| 跳过策略（mtime/size/ETag） | 20-concepts/skip-logic.md [TODO] |
| 退出码（0/2 的含义） | 20-concepts/exit-codes.md [TODO] |
