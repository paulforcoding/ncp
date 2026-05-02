# ncp

Agent-First file copy tool for massive-scale data migration.

ncp copies files to remote servers and cloud object storage with DB-backed progress tracking, structured Agent-First output, and precise resume capabilities.

## Features

- **Massive-scale copy** — Tested with 10M+ files. Pipeline architecture (Walker → Replicator → DBWriter) keeps memory flat.
- **DB-backed progress** — Every file's copy/checksum status is persisted to PebbleDB. Interrupt at any time; resume picks up exactly where you left off.
- **Agent-First output** — Structured JSON FileLog events (`copy_plan`, `copy_progress`, `copy_complete`, `cksum_complete`) designed for programmatic consumption.
- **Multiple backends** — Local filesystem, remote ncp server (`ncp://`), Alibaba Cloud OSS (`oss://`).
- **Multi-source copy** — `ncp copy src1 src2 dst/` copies each source into its own subdirectory under dst.
- **Checksum verification** — Independent `ncp cksum` command with MD5 or xxHash algorithms. Supports copy→cksum→copy cycles.
- **File mtime preservation** — Regular file modification times are preserved during copy; directory mtimes are restored after copy completes.
- **Protocol integrity** — Remote protocol (ncp://) uses CRC32 checksums on every frame to detect data corruption in transit.

## Install

```bash
make build
# Binary: ./ncp
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
| `--FileLogOutput` | console | FileLog output: console or file path |
| `--FileLogInterval` | 5 | FileLog output interval in seconds |
| `--ProgressStorePath` | ./progress | Progress storage directory |
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

## Copy-Check-Retry Workflow

```
1. ncp copy /src /dst              # Copy all files
2. ncp cksum --task <taskID>       # Verify copied files
3. ncp copy --task <taskID>        # Re-copy only mismatched files
```

Step 3 only re-copies files where `cksumStatus != pass`, because `ResumeFromDB` skips files with `CksumPass` or `CopyDone+CksumNone`.

## Development

```bash
make build          # Build binary
make test           # Run all tests
make unit           # Unit tests only
make integration    # Integration tests only
make lint           # Run golangci-lint
```

## License

MIT
