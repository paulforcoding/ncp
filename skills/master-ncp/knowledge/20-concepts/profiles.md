# Cloud Profiles

## 触发条件

当以下情况出现时读这个文件：
- 需要配置云存储凭据
- 遇到 profile not found / Provider mismatch 错误
- 需要理解 `${env:}` 占位符和 0600 权限检查
- 不确定 OSS/COS/OBS 各需要哪些字段

## 快速行动

在 `ncp_config.json` 中定义 profile，URL 通过 `<profile>@` 引用：

```json
{
  "Profiles": {
    "oss-prod": {
      "Provider": "oss",
      "Endpoint": "oss-cn-shenzhen.aliyuncs.com",
      "Region":   "cn-shenzhen",
      "AK":       "${env:NCP_PROD_AK}",
      "SK":       "${env:NCP_PROD_SK}"
    }
  }
}
```

```bash
oss://oss-prod@my-bucket/backup/
```

## 详情

### 配置文件分层

按优先级从低到高，后层**整体替换**前层的 profile（不字段级 merge）：

1. `/etc/ncp_config.json`
2. `~/ncp_config.json`
3. `./ncp_config.json`
4. 环境变量（`NCP_` 前缀）
5. CLI flags

### 各 Provider 必填字段

| 字段 | oss | cos | obs |
|------|-----|-----|-----|
| Provider | **oss** | **cos** | **obs** |
| Endpoint | **必填** | 可选（自动构造） | **必填** |
| Region | **必填** | **必填** | **必填** |
| AK | **必填** | **必填** | **必填** |
| SK | **必填** | **必填** | **必填** |

- COS 的 Endpoint 若不填，自动构造为 `https://<bucket>.cos.<region>.myqcloud.com`
- OBS 的 Endpoint 在 profile 校验阶段为必填

### Provider 必须等于 URL scheme

`oss://prod@...` 要求 `Profiles.prod.Provider == "oss"`，否则启动期报错。这防止了用 oss profile 访问 cos bucket 等误操作。

### `${env:VAR}` 占位符

AK/SK/Endpoint/Region 支持 `${env:VAR_NAME}` 格式，加载期解析为环境变量值：

```json
"AK": "${env:NCP_PROD_AK}"
```

- 只有精确匹配 `${env:NAME}` 才会替换，部分匹配或格式错误保持原值
- 用 `ncp config show` 查看解析后的值（AK/SK 自动脱敏）

### 0600 权限检查

包含明文 AK/SK（非 `${env:...}` 引用）的配置文件**必须为 0600 权限**，否则 ncp 拒绝启动：

```bash
chmod 0600 ncp_config.json
```

使用 `${env:}` 引用 AK/SK 的配置文件不受此限制。

### 跨账号/跨区域

为每个账号定义独立 profile，URL 各自带 profile：

```bash
ncp copy oss://acct-a@bkt-a/data/ oss://acct-b@bkt-b/data/
ncp copy cos://acct-a@src-bucket/data/ cos://acct-b@dst-bucket/backup/
```

### 检查配置

```bash
ncp config show                    # 查看所有配置
ncp config show --profile <name>   # 查看指定 profile
ncp copy <src> <dst> --dry-run     # 预览执行配置
```

## 关联文件

- url-schemes.md — 各协议 URL 格式
- copy.md — copy 命令
