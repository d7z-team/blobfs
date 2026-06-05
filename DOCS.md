# BlobFS 设计文档

BlobFS 是一个本地海量对象存储库。核心目标是用简单的 Go 代码实现内容寻址、去重、可恢复 metadata、append-only 数据文件、显式目录和可控 GC，metadata log 直接内建于本地目录。

## 设计目标

- 数据内容按 chunk 做 CAS 存储，读路径校验 chunk hash。
- 文件系统 metadata 与物理数据分离，前台写入只提交短事务。
- segment append 写入，将 payload 聚合到顺序写的大文件中。
- 数据生命周期由 metadata tombstone、GC 和 compaction 管理。
- 海量目录操作采用 metadata-only rename 和 detach 语义。
- 恢复和观测提供薄 struct API，调用方可按需封装 HTTP、Prometheus 或 OpenTelemetry。

## 核心模型

```text
tenant/path
  -> inode
  -> manifest
  -> manifest chunk refs
  -> chunk metadata
  -> segment record
  -> compressed payload
```

主要记录：

- `tenant`: tenant id 到 root inode 的映射。
- `inode`: 文件或目录记录，包含父 inode、名称、大小、manifest、mode、mtime、generation 等。
- `dir_entry`: 目录 inode 到子名称和子 inode 的索引。
- `manifest`: 文件内容快照，保存 chunk refs 和文件 hash。
- `chunk`: CAS chunk 元数据，保存 segment 位置、大小、refcount、状态和校验信息。
- `segment`: 物理 segment 文件元数据，保存路径、写入偏移、状态和时间戳。

状态集合：

```text
inode:    ACTIVE, DELETED
manifest: ACTIVE, DELETED
chunk:    ACTIVE, GARBAGE_CANDIDATE, DELETED, CORRUPT
segment:  SEALED, COMPACTING, DELETED, CORRUPT
```

## 存储布局

```text
base/
  meta/
    LOCK
    SUPER0
    SUPER1
    checkpoint.json
    txlog/
      000001.log
      000002.log
  data/
    segments/
      0000/
        0000/
          0000000000000001.blob
    staging/
      sessions/
```

`segments` 使用固定两级 fanout，每级 1024 桶。segment 文件按 record 追加写入，payload 通过新 segment 表达更新。staging 目录用于写入前的临时 segment 和 VFS write session。

## Chunk 与 Segment

写入流经过 FastCDC-style 内容定义切分。默认 chunk 参数：

```text
min: 512 KiB
avg: 4 MiB
max: 16 MiB
```

chunk id 由 SHA-256 计算：

```text
DedupScopeTenant: sha256(tenant_id || bytes)
DedupScopeGlobal: sha256(bytes)
```

segment record 内容：

```text
record_magic
record_version
record_type
chunk_id
raw_size
stored_size
compression
checksum_crc32c
payload_length
payload
```

payload 使用 zstd 压缩。读取 chunk 时校验 record header、payload length、CRC32C、解压后大小和 CAS chunk hash；校验通过后返回数据，校验失败会报告读取错误。

## Metadata 持久化

metadata 使用 checkpoint + append-only txlog：

```text
checkpoint.json
txlog/<active-generation>.log
SUPER0 / SUPER1
```

事务提交顺序：

```text
write txlog frame
fsync txlog
apply in-memory metadata
update SUPER checkpoint txid/log generation
```

txlog frame 包含 magic、payload size、CRC 和 JSON transaction。完整 frame CRC 错误会使打开流程进入错误返回；崩溃造成的 torn tail 会按最后一个完整 frame 恢复，并通过 `Health` / `Diagnose` 报告 degraded warning。

checkpoint 会写入紧凑 metadata snapshot，创建并同步新一代空 txlog，然后通过 `SUPER0` / `SUPER1` 切换活动 log。旧 log 在切换成功后删除；checkpoint 失败时继续使用原活动 txlog。

checkpoint compaction 会清理已经完成生命周期的 deleted inode、manifest、chunk、segment，并裁剪 GC recent history。GC 总运行次数和最后 epoch 单独保存，让 checkpoint 大小保持有界。

## 写入流程

```text
1. 校验 context、tenant、path、reader 和配置限制
2. 流式切分 chunk，计算 file hash 和 chunk hash
3. 已存在且可读的 chunk 会被 pin 后复用
4. 新 chunk 写入 staging segment
5. staging segment fsync 后 rename 到 data/segments
6. metadata 短事务校验父目录、generation 和复用 chunk 可读性
7. 提交 segment、chunk、manifest、inode、dir_entry
8. 达到阈值后 checkpoint
```

写入顺序约束：

```text
payload durable -> segment visible -> metadata durable -> inode visible
```

metadata 提交失败时，对象保持未发布状态；已准备的 segment 会被清理，清理失败会随错误返回。

## 读取流程

`OpenObject` 会在打开时复制 manifest/chunk/segment 快照，并 pin 对应 segment，保证 reader 生命周期内的 segment 保持可读。`ObjectReader` 顺序读优先命中当前或下一个 chunk，随机 seek 使用二分定位 chunk。

读取流程：

```text
resolve inode
load manifest refs
validate chunk and segment state
pin segments
read segment payload
verify CRC, size, chunk hash
return bytes
```

`OpenRange` 使用相同快照语义，只限制 reader 的 logical range。

## 目录与 VFS

BlobFS 的目录是显式 inode 和 dentry，父目录由 `Mkdir` / `MkdirAll` 创建。写入 `a/b.txt` 前先创建 `a`。

`*Store` 实现 `afero.Fs`，路径格式为：

```text
tenant_id/path/to/file
```

`TenantFS(tenantID)` 返回以 tenant 为根的只读 `io/fs.FS`。

VFS 写入使用 write session：

```text
OpenFile/OpenFileContext
  -> data/staging/sessions/session-*.tmp
  -> copy old content when needed
  -> Write/WriteAt/Truncate update session
  -> Sync or Close commits through Put path
```

`MaxOpenWriteSessions` 限制同时打开的写 session。提交使用 base generation 检查；如果文件在句柄打开后被其他写入修改，返回 `ErrConflict`。`OpenFileContext` 的 context 会用于打开阶段和后续 `Sync` / `Close` 提交。

目录 rename 更新目录 inode 的父指针和 dentry，子树保持原 inode 结构。子路径通过 inode 父链解析。`RemoveAll` 立即删除父目录项并 tombstone 顶层 inode，子树内脱离目录树的 inode 和引用由后续 GC 释放。

## 删除、GC 与 Compaction

删除先更新 metadata，物理数据由 GC 在后续周期回收：

```text
inode -> DELETED
manifest refcount--
chunk refcount--
```

GC 流程：

```text
1. 标记 GC 可回收 inode
2. 根据 active manifest 统计 live chunk
3. 未引用 chunk 进入 GARBAGE_CANDIDATE
4. 达到确认轮数后 chunk -> DELETED
5. 根据 segment 垃圾比例选择 compaction candidate
6. 将 live chunks 重写到新 segment
7. fully dead segment 到达删除延迟后删除文件并标记 DELETED
```

segment pin 会保护正在被 reader 或 prepared write 复用的 segment。compaction 结果提交时会再次校验 source segment 和 chunk 位置，保证并发状态变化时迁移结果可回滚。

默认 GC 配置：

```text
SafetyWindow: 24h
CandidateConfirmCycles: 2
SegmentDeleteDelay: 24h
CompactGarbageRatio: 0.6
```

GC duration 字段中，`0` 表示使用默认值，小于 `0` 的值表示显式关闭对应等待窗口。测试或本地工具需要立即回收时可使用小于 `0` 的值。

## 恢复与观测 API

BlobFS 暴露薄 API，调用方可自行封装服务端、handler 和指标系统：

```go
Health(ctx)
Stats(ctx)
Diagnose(ctx, opts)
Repair(ctx, opts)
RemoveStaleLock(baseDir)
RemoveFSStaleLock(fs, baseDir)
```

`Health` 做轻量 metadata 和路径可用性检查。它会报告 store 状态、metadata 加载状态、txlog 写入状态、checkpoint 健康状态、corrupt/compacting 状态，以及 torn txlog tail replay 状态。

`Stats` 聚合内存 metadata，用于获取租户、inode、manifest、chunk、segment、字节和 GC 计数。

`Diagnose` 默认 dry-run 语义，可选扫描：

```text
CheckFiles:   metadata 引用的 segment 文件是否存在
CheckOrphans: data/segments 下未被 metadata 引用的文件
CheckStaging: data/staging 残留文件
MaxIssues:    返回问题数量上限
```

`Repair` 面向低风险动作，默认 dry-run；设置 `Apply: true` 后执行修改：

```text
CleanStaging
CleanOrphans
ResetCompacting
MarkMissingCorrupt
```

txlog 截断、manifest 重建、缺失 chunk 内容重建属于调用方显式恢复流程。异常退出留下的 `LOCK` 会保护 store 独占打开语义；确认 store 所有权后，调用 `RemoveStaleLock` 或 `RemoveFSStaleLock` 显式清理。

`StartBackground` 同一时间只允许一组后台 worker。`Close` 会先取消 store context，等待后台 GC 和已进入的操作结束，然后 checkpoint 并关闭 txlog。

## 配置

```go
type Config struct {
    SegmentSize          int64
    MaxFileSize          int64
    MaxTenantLength      int
    MaxPathLength        int
    MaxComponentLength   int
    MaxOpenWriteSessions int
    AllowExecutableFiles bool
    Compression          CompressionType
    Checksum             ChecksumType
    DedupScope           DedupScope
    Chunking             ChunkingConfig
    GC                   GCConfig
}

type ChunkingConfig struct {
    Algorithm string
    MinSize   int
    AvgSize   int
    MaxSize   int
}

type GCConfig struct {
    SafetyWindow           time.Duration
    CandidateConfirmCycles int
    SegmentDeleteDelay     time.Duration
    CompactGarbageRatio    float64
}
```

默认值：

```text
SegmentSize: 256 MiB
MaxFileSize: 1 TiB
MaxTenantLength: 128
MaxPathLength: 4096
MaxComponentLength: 255
MaxOpenWriteSessions: 1024
Compression: zstd
Checksum: crc32c
DedupScope: tenant
Chunking: FastCDC
GC.SafetyWindow: 24h
GC.CandidateConfirmCycles: 2
GC.SegmentDeleteDelay: 24h
GC.CompactGarbageRatio: 0.6
```

## 路径规则

```text
tenant_id 只能包含字母、数字、_、-、.
object path 必须是相对路径
path component 使用普通名称，保留 .、..、NUL 和绝对路径给系统语义
rename 在同一 tenant 内执行
文件写入目标是普通文件路径
目录子项创建在目录路径下执行
普通文件默认清除执行位
```

BlobFS 是本地库，stale lock 的所有权判定由调用方完成。删除 stale lock 前需确认 store 当前归属。
