# ncp

Agent-First file copy tool for massive-scale data migration with DB-backed resume.

ncp copies files to remote servers and cloud object storage with DB-backed progress tracking, structured Agent-First output, and precise resume capabilities.

## Features

- **Massive-scale copy** — Tested with 10M+ files. Pipeline architecture (Walker → Replicator → DBWriter) keeps memory flat.
- **DB-backed progress** — Every file's copy/checksum status is persisted to PebbleDB. Interrupt at any time; resume picks up exactly where you left off.
- **High performance** — DB-tracked resume with minimal overhead; batched writes and delayed flush to avoid impacting copy throughput.
- **Unique workflow** — Supports both copy-then-verify and verify-then-incremental-copy patterns, enabling efficient data synchronization.
- **Agent-First output** — Structured NDJSON FileLog events (`file_complete`, `file_metadata_complete`, `progress_summary`) designed for programmatic consumption by agents and scripts.
- **Multiple backends** — Local filesystem, remote ncp server (`ncp://`), Alibaba Cloud OSS (`oss://`).
- **Checksum verification** — Independent `ncp cksum` command with MD5 or xxHash algorithms. Supports copy→cksum→copy cycles.

## Notes
- Only regular files, directories, and symbolic links are supported. Pipes, sockets, device files, and other special file types are skipped.
- Supported metadata: mode, owner (uid/gid), mtime, xattr. ACLs and other special attributes are not supported.

## Install

```bash
make build
# Binary: ./ncp
```

## URL Schemes

ncp uses URL-style path prefixes to select the storage backend:

| Scheme | Syntax | Source | Destination | Example |
|--------|--------|--------|-------------|---------|
| *(none)* | `/path/to/dir` | Yes | Yes | `/data/project` |
| `ncp://` | `ncp://host:port/path` | No | Yes | `ncp://server:9900/backup` |
| `oss://` | `oss://<profile>@bucket/prefix/` | Yes | Yes | `oss://prod@my-bucket/backup/` |

**Constraints:**
- `ncp://` is destination-only (remote server receives pushes).
- `oss://` requires a `<profile>@` prefix referencing a profile defined in `ncp_config.json`. Local and `ncp://` URLs MUST NOT carry a profile.

### Path Semantics

`ncp copy` places **every** source under its basename as a subdirectory of the destination. This applies to single-source and multi-source copies alike.

```
ncp copy /data/project /backup/
# Result: /backup/project/...

ncp copy /data/logs /data/configs /backup/
# Result:
#   /backup/logs/...
#   /backup/configs/...

ncp copy oss://prod@my-bucket/ /backup/
# Result: /backup/my-bucket/...
```

Only **local paths** can be used as multi-source — mixing `oss://` or `ncp://` with other sources is not allowed.

```bash
# OK: multiple local sources
ncp copy /data/logs /data/configs /backup/

# ERROR: mixing local and OSS sources
ncp copy /data/logs oss://prod@bucket/prefix/ /backup/

# ERROR: multiple OSS sources
ncp copy oss://prod@bucket-a/data/ oss://prod@bucket-b/data/ /backup/
```

### Profiles (cloud credentials)

Cloud URLs reference a named profile via userinfo: `oss://<profile>@bucket/path/`. Profiles are defined in `ncp_config.json` under the `Profiles` key. The same name can map to different accounts or regions, which is how cross-account migration is expressed:

```bash
# Cross-account OSS copy: each side picks its own profile.
ncp copy oss://acct-a@bkt-a/data/ oss://acct-b@bkt-b/data/
```

`ncp_config.json` uses the layered search path (`/etc/ncp_config.json` → `~/ncp_config.json` → `./ncp_config.json`); later layers fully replace any profile they redefine (no field-level merging, so credentials never end up half new and half old).

```json
{
  "Profiles": {
    "prod": {
      "Provider": "oss",
      "Endpoint": "oss-cn-shenzhen.aliyuncs.com",
      "Region":   "cn-shenzhen",
      "AK":       "${env:NCP_PROD_AK}",
      "SK":       "${env:NCP_PROD_SK}"
    },
    "dr": {
      "Provider": "oss",
      "Endpoint": "oss-cn-beijing.aliyuncs.com",
      "Region":   "cn-beijing",
      "AK":       "${env:NCP_DR_AK}",
      "SK":       "${env:NCP_DR_SK}"
    }
  }
}
```

**Rules:**
- The profile referenced in a URL MUST exist in the loaded config; otherwise ncp fails fast at startup.
- `Provider` MUST equal the URL scheme (`oss://prod@...` requires `Profiles.prod.Provider == "oss"`).
- `${env:VAR}` placeholders in `AK`/`SK`/`Endpoint`/`Region` are resolved at load time. Plain credentials are accepted but force the config file to be `0600`; otherwise ncp refuses to start.
- There is no fallback: cloud URLs without a profile, or with embedded passwords, are rejected.

**Constraints:**
- `--cksum-algorithm` must be `md5` when OSS is involved (OSS uses Content-MD5 for integrity verification; `xxh64` is not supported).
- POSIX metadata (mode, uid, gid, mtime, symlink target, xattr) is preserved as custom object metadata with the `ncp-` prefix (e.g. `ncp-mode`, `ncp-uid`).

Example:

```bash
# Local → OSS
ncp copy /data/project oss://prod@my-bucket/backup/

# OSS → Local
ncp copy oss://prod@my-bucket/backup/ /data/restore/

# OSS → OSS (same account)
ncp copy oss://prod@src-bucket/data/ oss://prod@dst-bucket/backup/

# OSS → OSS (cross-account)
ncp copy oss://acct-a@src-bucket/data/ oss://acct-b@dst-bucket/backup/

# Verify OSS data
ncp cksum /data/project oss://prod@my-bucket/backup/
```

## Quick Start

```bash
# Copy local directory — result is /backup/project/...
ncp copy /data/project /backup/

# Copy multiple sources into one destination
ncp copy /data/logs /data/configs /backup/

# Copy to a remote ncp server — creates /backup/project/... on the server
ncp serve --base /backup --listen :9900 &  # on the destination server
ncp copy /data/project ncp://server:9900/backup/

# Copy to Alibaba Cloud OSS — creates backup/project/... under the bucket
ncp copy /data/project oss://prod@my-bucket/backup/

# Copy entire OSS bucket — creates /restore/my-bucket/...
ncp copy oss://prod@my-bucket/ /restore/

# Verify data consistency (both paths are explicit bases)
ncp cksum /data/project /backup/project
ncp cksum /data/project oss://prod@my-bucket/backup/project

# Resume an interrupted task
ncp resume task-20260502-143000-abcd

# Resume using specific command with task ID
ncp copy --task task-20260502-143000-abcd
```

## Commands

### `ncp copy <src>... <dst>`

Copy files from one or more sources to a destination. Supports local, `ncp://`, and `oss://` schemes.

**Path semantics:** Every source is placed under its basename as a subdirectory of `dst`. Both single-source and multi-source copies follow this rule.

| Flag | Default | Description |
|------|---------|-------------|
| `--CopyParallelism` | 1 | Number of parallel copy workers |
| `--IOSize` | 0 (tiered) | IO size in bytes |
| `--cksum-algorithm` | md5 | Checksum algorithm: `md5` or `xxh64` |
| `--enable-DirectIO` | false | Enable Direct IO (mutually exclusive with SyncWrites) |
| `--enable-SyncWrites` | true | Enable fsync on write |
| `--enable-EnsureDirMtime` | true | Restore directory mtime after copy |
| `--enable-FileLog` | true | Enable structured FileLog output |
| `--FileLogOutput` | /tmp/ncp_file_log.json | FileLog output: console or file path |
| `--FileLogInterval` | 5 | FileLog progress_summary interval in seconds |
| `--ProgressStorePath` | /tmp/ncp_progress_store | Progress storage directory |
| `--ProgramLogLevel` | info | Log level: trace/debug/info/warn/error/critical |
| `--dry-run` | false | Print effective config and exit |
| `--task` | | Resume existing task by taskID |

### `ncp cksum <src> <dst>`

Verify data consistency between source and destination by comparing checksums. Both `src` and `dst` are explicit base paths; no automatic basename joining is performed.

| Flag | Default | Description |
|------|---------|-------------|
| `--cksum-algorithm` | md5 | Checksum algorithm: `md5` or `xxh64` |
| `--CopyParallelism` | 1 | Number of parallel checksum workers |
| `--task` | | Resume existing cksum task by taskID |

### `ncp resume <taskID>`

Resume an interrupted copy or checksum task. Auto-detects job type from the task's last run.

### `ncp serve`

Start an ncp protocol server to receive file pushes.

| Flag | Default | Description |
|------|---------|-------------|
| `--listen` | :9900 | Listen address |
| `--base` | | Base directory for received files (required) |

### `ncp task`

Manage tasks: `list`, `show <taskID>`, `delete <taskID>`, `migrate-profile <taskID>`.

`migrate-profile` rewrites the `srcBase`/`dstBase` URLs in a task's `meta.json` to add a `<profile>@` prefix. Use it once after upgrading from a pre-profile build of ncp; afterwards `ncp resume <taskID>` works again. Local paths and `ncp://` URLs are left unchanged.

```bash
# Apply the same profile to both src and dst
ncp task migrate-profile <taskID> --profile prod

# Different profiles per side (cross-account migration)
ncp task migrate-profile <taskID> --src-profile acct-a --dst-profile acct-b
```

### `ncp profile`

Inspect profiles defined in `ncp_config.json`.

```bash
ncp profile list           # name<TAB>provider<TAB>region per profile
ncp profile show <name>    # full profile JSON with AK/SK masked
```

## Architecture

```
Walker(1) ──discoverCh──→ Replicator(N) ──resultCh──→ DBWriter(1)
```

- **Walker** traverses the source directory, writes progress to PebbleDB, and pushes items to the discovery channel.
- **Replicator** (N workers) copies files from source to destination, computing checksums in-stream.
- **DBWriter** batches results and persists them to PebbleDB.
- **Back-pressure**: When the channel is full, Walker writes to DB only and replays after walk completes.

Progress is stored as 2-byte values `[CopyStatus][CksumStatus]` keyed by relative path, with a `__walk_complete` sentinel for resume decisions.

## Usage Scenarios

### Scenario 1: Copy first, then verify (ensure copy correctness)

Use when you have a completed copy and want to verify data integrity.

```bash
# 1. Copy files
ncp copy /data/project /backup/project
# Output contains taskId, e.g. task-20260502-143000-abcd

# 2. Verify copied files
ncp cksum --task task-20260502-143000-abcd

# 3. If mismatches found, re-copy only the failed files
ncp copy --task task-20260502-143000-abcd

# Or use resume to auto-detect the last job type
ncp resume task-20260502-143000-abcd
```

Step 3 only re-copies files where `cksumStatus != pass`. `ResumeFromDB` skips files with `CksumPass` or `CopyDone+CksumNone`.

### Scenario 2: Verify first, then incremental copy (sync based on existing data)

Use when the destination already has partial data — verify first to find differences, then copy only what's needed.

```bash
# 1. Check differences between source and destination
ncp cksum /data/project /backup/project
# Output contains taskId, e.g. task-20260502-150000-ef01

# 2. Based on verification results, copy only mismatched files
ncp copy --task task-20260502-150000-ef01
# copy skips files with cksumStatus=pass, only copies mismatch/error/none files
```

This pattern is ideal for incremental sync: use cksum to quickly locate divergent files, then use copy to precisely fill the gaps — avoiding a full redundant copy.

## FileLog

FileLog is ncp's structured event stream — every file operation emits a JSON line, making it easy for agents and scripts to track progress in real time.

### Event Format

All events are NDJSON (one JSON object per line). Every event contains:

```json
{"timestamp": "2026-05-03T14:30:00.123456789Z", "event": "<type>", "taskId": "task-20260502-143000-abcd", ...}
```

### Event Types

#### `file_complete` — content-level event, emitted per regular file after batch flush

Reports the result of **content copy** (write data + close file) for regular files. One event per file.

Copy mode:

```json
{
  "timestamp": "2026-05-03T14:30:01.234567890Z",
  "event": "file_complete",
  "taskId": "task-20260502-143000-abcd",
  "action": "copy",
  "result": "done",
  "errorCode": "",
  "relPath": "src/main.go",
  "fileType": "regular",
  "fileSize": 4096,
  "algorithm": "md5",
  "checksum": "d41d8cd98f00b204e9800998ecf8427e"
}
```

- `result`: `"done"`, `"error"`, or `"skipped"`. `"skipped"` means the file was skipped by mtime/size/ETag match.
- `checksum`: hex string of the in-stream checksum (content copy only).

Checksum mode:

```json
{
  "timestamp": "2026-05-03T14:35:00.345678901Z",
  "event": "file_complete",
  "taskId": "task-20260502-150000-ef01",
  "action": "cksum",
  "result": "done",
  "errorCode": "",
  "relPath": "src/main.go",
  "fileType": "regular",
  "fileSize": 4096,
  "algorithm": "md5",
  "checksum": "",
  "srcHash": "d41d8cd98f00b204e9800998ecf8427e",
  "dstHash": "d41d8cd98f00b204e9800998ecf8427e"
}
```

- `result`: `"done"` (pass) or `"error"` (mismatch/error). `srcHash` and `dstHash` are populated for regular files.

#### `file_metadata_complete` — metadata-level event, emitted per file after batch flush

Reports the result of **metadata operations** (Mkdir / Symlink / SetMetadata) for all file types. One event per file.

```json
{
  "timestamp": "2026-05-03T14:30:01.234567890Z",
  "event": "file_metadata_complete",
  "taskId": "task-20260502-143000-abcd",
  "result": "done",
  "errorCode": "",
  "relPath": "src/main.go",
  "fileType": "regular"
}
```

- Dirs and symlinks **only** emit this event (no `file_complete`).
- Regular files emit both `file_complete` (content) and `file_metadata_complete` (metadata).
- `result`: `"done"` or `"error"`.

#### `progress_summary` — emitted periodically (controlled by `--FileLogInterval`)

```json
{
  "timestamp": "2026-05-03T14:30:05.456789012Z",
  "event": "progress_summary",
  "taskId": "task-20260502-143000-abcd",
  "phase": "copy",
  "finished": false,
  "exitCode": 0,
  "walker": {
    "walkComplete": true,
    "discoveredCount": 1000000,
    "dispatchedCount": 500000,
    "backlogCount": 500000,
    "channelFull": false
  },
  "replicator": {
    "filesCopied": 480000,
    "bytesCopied": 107374182400,
    "filesPerSec": 3200.5,
    "bytesPerSec": 715827882.7
  },
  "dbWriter": {
    "pendingCount": 50,
    "totalDone": 480000,
    "totalFailed": 3,
    "totalProcessed": 480003
  }
}
```

- `phase`: `"copy"` or `"cksum"`.
- `finished`: `true` on the final summary when the job completes.
- `exitCode`: only meaningful when `finished=true`. `0` = all pass, `2` = errors/mismatches.

### Using FileLog

**For agents** — tail the FileLog file and react to events:

```bash
# Watch for mismatches in real time
tail -f /tmp/ncp_file_log.json | jq 'select(.event=="file_complete" and .result=="error")'

# Track progress
tail -f /tmp/ncp_file_log.json | jq 'select(.event=="progress_summary") | {phase, filesCopied: .replicator.filesCopied, bytesPerSec: .replicator.bytesPerSec}'

# Detect job completion
tail -f /tmp/ncp_file_log.json | jq 'select(.event=="progress_summary" and .finished==true)'
```

**For humans** — pipe through `jq` for readable output:

```bash
# Show each completed file
cat /tmp/ncp_file_log.json | jq -c '{event, relPath: .relPath, result: .result}'

# Show only errors
cat /tmp/ncp_file_log.json | jq 'select(.result=="error")'
```

**Configuration**:

| Flag | Default | Description |
|------|---------|-------------|
| `--enable-FileLog` | true | Enable/disable FileLog |
| `--FileLogOutput` | /tmp/ncp_file_log.json | Output destination: `console` for stdout, or a file path |
| `--FileLogInterval` | 5 | Seconds between `progress_summary` events |

## Development

```bash
make build          # Build binary
make test           # Run all tests
make unit           # Unit tests only
make integration    # Integration tests only
make lint           # Run golangci-lint
```

## License

GNU General Public License v3.0
