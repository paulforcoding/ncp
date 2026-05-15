# FIX-004: 修复 cksum→copy 闭环工作流

**修复日期**: 2026-05-16

## 问题 1 修复: copy --task Cobra 参数验证

**文件**: `cmd/ncp/main.go:45-56`

将 `copyCmd.Args` 从 `cobra.MinimumNArgs(2)` 改为自定义验证函数：

```go
Args: func(cmd *cobra.Command, args []string) error {
    taskID, _ := cmd.Flags().GetString("task")
    if taskID != "" {
        return nil
    }
    if len(args) < 2 {
        return fmt.Errorf("copy requires <src> and <dst> arguments when not using --task")
    }
    return nil
},
```

`cksumCmd` 使用 `cobra.MaximumNArgs(2)` 已允许 0 参数，无需修改。

## 问题 2 修复: copy/cksum --task 跨任务 resume 的 relPath 格式不匹配

### 根因

copy 使用 `BasenamePrefixedSource` 包装 source，DB relPath 带有 basename 前缀（如 `project/file1`）。
cksum 使用普通 source，DB relPath 不带前缀（如 `file1`）。

当跨任务 resume 时，DB relPath 格式与 source 路由格式不匹配，导致 `src.Stat()` 全部失败（被静默跳过）。

### 修复方案

新增 `firstRunJobType(meta)` 函数获取任务首次运行的类型，resume 时根据首次运行类型选择匹配的 source 设置：

1. **`runCopyResume` / `runResumeCopy`**:
   - 首次运行是 copy → 使用 `setupCopyDepsMulti`（BasenamePrefixedSource）← 现有行为
   - 首次运行是 cksum → 使用 `setupCopyDepsPlain`（无 basename 前缀）

2. **`runCksumResume` / `runResumeCksum`**:
   - 首次运行是 cksum → 使用 `setupCksumDeps`（普通 source）← 现有行为
   - 首次运行是 copy → 使用 `setupCopyDepsMulti` 创建 source（BasenamePrefixedSource），dst 用普通 source

3. **新增 `setupCopyDepsPlain`**: 创建不带 BasenamePrefixedSource 包装的 source 和 destination

4. **`setupCopyDepsMulti` 中 `case "oss":` → `case "oss", "cos", "obs":`**: 统一云存储 factory 处理

## 问题 3 修复: ResumeFromDBForCksum 只看 CksumStatus

**文件**: `internal/copy/walker.go:204-210`

移除 `cs != model.CopyDone` 检查，只根据 CksumStatus 决定是否需要 cksum：

```go
// Before:
cs, cks := it.Value()
if cs != model.CopyDone {
    continue
}
if cks == model.CksumPass {

// After:
_, cks := it.Value()
if cks == model.CksumPass {
```

理由：cksum-first 场景下，walker 写入的是 `CopyDispatched + CksumNone`，原逻辑会跳过所有非 CopyDone 的文件，导致 cksum resume 不工作。cksum resume 只关心 CksumStatus，与 CopyStatus 无关。

## 测试

- 新增 `TestFirstRunJobType` 验证首次运行类型检测
- 全部单元测试通过（17 packages）
