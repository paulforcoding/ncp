# ncp

Agent-First file copy tool for massive-scale data migration with DB-backed resume.

ncp copies files to remote servers and cloud object storage with DB-backed progress tracking, structured Agent-First output, and precise resume capabilities.

## Features

- **Massive-scale copy** — Tested with 10M+ files. Pipeline architecture (Walker → Replicator → DBWriter) keeps memory flat.
- **DB-backed progress** — Every file's copy/checksum status is persisted to PebbleDB. Interrupt at any time; resume picks up exactly where you left off.
- **High performance** — DB-tracked resume with minimal overhead; batched writes and delayed flush to avoid impacting copy throughput.
- **Unique workflow** — Supports both copy-then-verify and verify-then-incremental-copy patterns, enabling efficient data synchronization.
- **Agent-First output** — Structured NDJSON FileLog events (`copy_plan`, `file_complete`, `progress_summary`) designed for programmatic consumption by agents and scripts.
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
| `oss://` | `oss://bucket/prefix/` | Yes | Yes | `oss://my-bucket/backup/` |

**Constraints:**
- `ncp://` is destination-only (remote server receives pushes).
- `ncp cksum` does not support `ncp://` destinations — the protocol has built-in MD5 verification.
- `oss://` paths require additional OSS parameters (`--endpoint`, `--region`, `--access-key-id`, `--access-key-secret`).

### Multi-Source Copy

`ncp copy` accepts multiple source paths. Only **local paths** can be used as multi-source — mixing `oss://` or `ncp://` with other sources is not allowed.

```bash
# OK: multiple local sources
ncp copy /data/logs /data/configs /backup/

# ERROR: mixing local and OSS sources
ncp copy /data/logs oss://bucket/prefix/ /backup/

# ERROR: multiple OSS sources
ncp copy oss://bucket-a/data/ oss://bucket-b/data/ /backup/
```

Each source's files appear under its directory name in the destination:

```
ncp copy /data/logs /data/configs /backup/
# Result:
#   /backup/logs/...
#   /backup/configs/...
```

### OSS Configuration

When using `oss://` paths (as source or destination), you must provide Alibaba Cloud OSS credentials via CLI flags or config file:

| Flag | Required | Description |
|------|----------|-------------|
| `--endpoint` | Yes | OSS endpoint, e.g. `oss-cn-shenzhen.aliyuncs.com` |
| `--region` | Yes | OSS region, e.g. `cn-shenzhen` |
| `--access-key-id` | Yes | Alibaba Cloud AccessKey ID |
| `--access-key-secret` | Yes | Alibaba Cloud AccessKey Secret |

The same set of OSS parameters applies to the entire command — if both source and destination are `oss://`, they must share the same endpoint/region/credentials (i.e., be in the same OSS region).

**Constraints:**
- `--cksum-algorithm` must be `md5` when OSS is involved (OSS uses Content-MD5 for integrity verification; `xxh64` is not supported).
- POSIX metadata (mode, uid, gid, mtime, symlink target, xattr) is preserved as custom object metadata with the `ncp-` prefix (e.g. `ncp-mode`, `ncp-uid`).

Example:

```bash
# Local → OSS
ncp copy /data/project oss://my-bucket/backup/ \
  --endpoint oss-cn-shenzhen.aliyuncs.com \
  --region cn-shenzhen \
  --access-key-id YOUR_AK \
  --access-key-secret YOUR_SK

# OSS → Local
ncp copy oss://my-bucket/backup/ /data/restore/ \
  --endpoint oss-cn-shenzhen.aliyuncs.com \
  --region cn-shenzhen \
  --access-key-id YOUR_AK \
  --access-key-secret YOUR_SK

# OSS → OSS (same region)
ncp copy oss://src-bucket/data/ oss://dst-bucket/backup/ \
  --endpoint oss-cn-shenzhen.aliyuncs.com \
  --region cn-shenzhen \
  --access-key-id YOUR_AK \
  --access-key-secret YOUR_SK

# Verify OSS data
ncp cksum /data/project oss://my-bucket/backup/ \
  --endpoint oss-cn-shenzhen.aliyuncs.com \
  --region cn-shenzhen \
  --access-key-id YOUR_AK \
  --access-key-secret YOUR_SK
```

## Quick Start

```bash
# Copy local directory to another local path
ncp copy /data/project /backup/project

# Copy multiple sources into one destination
ncp copy /data/logs /data/configs /backup/

# Copy to a remote ncp server
ncp serve --base /backup --listen :9900 &  # on the destination server
ncp copy /data/project ncp://server:9900/backup/project

# Copy to Alibaba Cloud OSS
ncp copy /data/project oss://my-bucket/backup/ \
  --endpoint oss-cn-shenzhen.aliyuncs.com \
  --region cn-shenzhen \
  --access-key-id YOUR_AK \
  --access-key-secret YOUR_SK

# Verify data consistency
ncp cksum /data/project /backup/project
ncp cksum /data/project oss://my-bucket/backup/ --endpoint ... --region ...

# Resume an interrupted task
ncp resume task-20260502-143000-abcd

# Resume using specific command with task ID
ncp copy --task task-20260502-143000-abcd
```

## Commands

### `ncp copy <src>... <dst>`

Copy files from one or more sources to a destination. Supports local, `ncp://`, and `oss://` schemes.

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
| `--endpoint` | | OSS endpoint |
| `--region` | | OSS region |
| `--access-key-id` | | OSS AccessKey ID |
| `--access-key-secret` | | OSS AccessKey Secret |

### `ncp cksum <src> <dst>`

Verify data consistency between source and destination by comparing checksums.

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

Manage tasks: `list`, `show <taskID>`, `delete <taskID>`.

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

#### `copy_plan` — emitted once at job start

```json
{
  "timestamp": "2026-05-03T14:30:00.123456789Z",
  "event": "copy_plan",
  "taskId": "task-20260502-143000-abcd",
  "sources": ["/data/project"],
  "dest": "/backup/project",
  "algorithm": "md5"
}
```

#### `file_complete` — emitted per file after batch flush

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

- `result`: `"done"` or `"error"`. When `"error"`, `errorCode` contains the error message.
- `skipped`: present and `true` when the file was skipped by mtime/size/ETag match (only if `--skip-by-mtime` is enabled, which is the default).

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
