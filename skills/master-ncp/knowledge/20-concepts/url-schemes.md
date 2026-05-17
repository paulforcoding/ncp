# URL 格式

## 触发条件

当以下情况出现时读这个文件：
- 不确定 ncp 支持哪些存储后端 URL
- 需要构造 oss:// / cos:// / obs:// / ncp:// URL
- 遇到 profile 相关错误

## 快速行动

| 协议 | 格式 | 示例 |
|------|------|------|
| 本地 | 裸路径 | `/data/project` |
| ncp:// | `ncp://host:port/path` | `ncp://192.168.1.188:9900/backup/` |
| oss:// | `oss://<profile>@bucket/path/` | `oss://prod@my-bucket/backup/` |
| cos:// | `cos://<profile>@bucket/path/` | `cos://prod@my-bucket-1250000000/backup/` |
| obs:// | `obs://<profile>@bucket/path/` | `obs://prod@my-bucket/backup/` |

## 详情

### 各协议约束

| 协议 | 可作源 | 可作目标 | 需要 profile | 多源支持 |
|------|--------|---------|-------------|---------|
| 本地 | 是 | 是 | 否 | 是 |
| ncp:// | 是 | 是 | 否 | 否 |
| oss:// | 是 | 是 | **是** | 否 |
| cos:// | 是 | 是 | **是** | 否 |
| obs:// | 是 | 是 | **是** | 否 |

### ncp:// URL

- 格式：`ncp://<host>:<port>/<basePath>`
- 本地和 ncp:// URL **不能**带 userinfo（`@` 符号）
- 可作源也可作目标
- 使用前必须先在远端启动 `ncp serve`

### 云存储 URL

- **必须**带 `<profile>@` 前缀，否则 ncp 启动期立即报错
- profile 在 `ncp_config.json` 的 `Profiles` 字段定义
- URL 中不能嵌入密码，只引用 profile 名
- `Profiles.<name>.Provider` 必须等于 URL scheme（如 `oss://prod@...` 要求 `Profiles.prod.Provider == "oss"`）

### 本地路径

- 不含 `://` 的路径视为本地路径
- 相对路径会被自动转为绝对路径
- 可作源也可作目标

### 常见错误

| 错误 | 原因 |
|------|------|
| `scheme "oss" requires a profile` | 云 URL 缺少 `<profile>@` 前缀 |
| `profile "xxx" referenced in URL is not defined` | ncp_config.json 中没有该 profile |
| `Provider="oss" does not match URL scheme="cos"` | profile 的 Provider 与 URL scheme 不匹配 |
| `embedding password in URL is not allowed` | URL 中不能嵌入密码 |
| `scheme "ncp" does not accept a profile` | ncp:// URL 不能带 `@` userinfo |

## 关联文件

- profiles.md — 云存储凭据配置详解
- copy.md — copy 命令的路径语义
