# BlobFS 核心设计

BlobFS 是一个本地内容寻址文件存储库。它把文件抽象为 manifest，把文件内容切成 chunk，并把所有 chunk 追加写入 segment 文件。`Store` 直接实现 `github.com/spf13/afero.Fs`，可以作为 VFS 使用。

## 架构

```text
Client / afero.Fs
  -> Store API
      -> Metadata
          -> Manifest
              -> Chunk[]
                  -> Segment record
```

逻辑关系：

```text
file path -> file record -> manifest -> chunk refs -> chunk metadata -> segment location
```

物理数据只写入 segment；元数据记录 file、manifest、chunk、segment 的状态和位置。

租户是路径和去重的隔离边界。VFS 路径格式是：

```text
tenant_id/path/to/file
```

标准库 `io/fs` 消费方使用 `TenantFS(tenantID)`，得到以该租户为根的只读 `fs.FS` 视图。

## 文件切分

所有写入都使用同一个流式内容定义切分器，不再维护独立的小文件阈值。输入小于当前 `MaxSize` 时会自然形成单 chunk manifest；更大的输入按 FastCDC-style 边界切分。默认参数：

```text
min chunk: 512 KiB
avg chunk: 4 MiB
max chunk: 16 MiB
```

## 去重

默认租户内去重：

```text
DedupScopeTenant:
  chunk_id = sha256(tenant_id || bytes)

DedupScopeGlobal:
  chunk_id = sha256(bytes)
```

同租户、同内容、同大小的文件会复用 manifest。大文件的重复内容会复用已有 chunk。

## Segment 存储

目录布局：

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
```

规则：

- 只有一个 blob 根目录。
- segment 使用固定两级 fanout，每级 1024 桶。
- segment 文件名是纯数字 `.blob`。
- segment 文件权限不带执行位。
- payload 使用 zstd 压缩。
- record 使用 CRC32C 校验压缩后的 payload。

segment record：

```text
record_magic
record_version
record_type
chunk_id
raw_size
stored_size
compression
checksum
payload_length
payload
```

读取时会校验 record header、payload length、CRC32C、解压后大小和 chunk hash。

## Metadata

当前元数据存储为 typed metadata transaction log：

```text
meta/SUPER0
meta/SUPER1
meta/checkpoint.json
meta/txlog/<active-generation>.log
```

核心记录：

```text
tenants:
  tenant_id -> root inode

inodes:
  inode_id
  tenant_id
  name
  parent_inode
  kind
  size
  file_hash
  manifest_id
  state
  options
  mode
  mod_time
  uid
  gid
  generation
  content_generation
  metadata_generation
  ctime
  mtime
  atime
  created_at
  updated_at
  deleted_at

dir_entries:
  parent_inode -> child_name -> child_inode

manifests:
  manifest_id
  tenant_id
  file_size
  file_hash
  chunk_count
  chunking_type
  chunks
  state
  ref_count
  created_at
  last_live_at
  deleted_at

chunks:
  chunk_id
  tenant_id
  raw_size
  stored_size
  ref_count
  state
  segment_id
  segment_offset
  segment_length
  checksum_crc32c
  compression
  garbage_candidate_at
  garbage_seen_count
  deleted_at
  corruption fields

segments:
  segment_id
  relative_path
  write_offset
  total_bytes
  state
  timestamps
```

状态：

```text
file:      ACTIVE, DELETED
file kind: FILE, DIR
manifest:  ACTIVE, DELETED
chunk:     ACTIVE, GARBAGE_CANDIDATE, DELETED, CORRUPT
segment:   SEALED, COMPACTING, DELETED, CORRUPT
```

目录是显式 inode/dentry 记录。BlobFS 不从 `a/b.txt` 自动合成 `a`，所以写入嵌套 object 前必须先创建父目录。目录列表只依赖 `dir_entries`，不扫描全量文件名。目录 rename 移动的是父目录项和目录 inode，不扫描或改写子树。

`checkpoint.json` 是周期性 checkpoint，用来压缩 metadata 并限制启动 replay 成本。事务先写入当前 txlog 并 fsync；checkpoint 成功后写入紧凑 snapshot，创建并同步新一代空 txlog，再通过 `SUPER0`/`SUPER1` 切换活动 log。旧 log 只在切换成功后关闭和删除，因此 checkpoint 失败不会破坏仍可继续提交的旧 txlog。

checkpoint 会裁剪不再需要的 deleted inode、manifest、chunk、segment 记录。GC 历史保存总运行次数、最后 epoch 和最近窗口，避免长期后台 GC 让 checkpoint JSON 单调膨胀。

## 写入

```text
1. 校验 context、tenant、path 和 reader
2. 流式读取输入并执行 FastCDC chunking
3. 计算 scoped file hash 和 chunk hash
4. 将缺失 chunk 写入 staging segment
5. fsync segment 并 rename 到可见 data/segments
6. 短 metadata transaction 校验父目录和 generation
7. 写 chunk、manifest、inode 和 dentry transaction
8. 周期性更新 checkpoint/SUPER 以压缩 metadata log
```

顺序要求：

```text
staging segment payload -> visible segment -> metadata txlog -> inode visible
```

这样 inode 可见时，它引用的 chunk 已经可读。

VFS 写入使用临时 write session：

```text
OpenFile/OpenFileContext writable
  -> 创建 data/staging/sessions/session-*.tmp
  -> 受 MaxOpenWriteSessions 限制，避免无限临时写会话放大
  -> 复制旧内容或从空文件开始
  -> Write/WriteAt/Truncate 修改 session
  -> File.Sync 或 Close 执行一次带 base generation 的提交
```

如果打开句柄后同一路径被其他写入修改，提交会返回 `ErrConflict`。新建句柄在关闭前如果同路径已经被创建，也会返回 `ErrConflict`。`OpenFileContext` 提供最薄的可取消 VFS 写接口：传入的 context 会用于打开阶段读取旧内容，以及之后的 `Sync`/`Close` 提交。

## 读取

普通读取：

```text
file -> manifest -> chunks -> segment payload -> decompress -> stream
```

Range 读取：

```text
1. 根据 offset 和 length 定位相关 chunk
2. 读取必要 chunk
3. 解压后裁剪首尾
4. 返回 reader
```

读取会拒绝不可读的 chunk 和 segment 状态，并在返回 bytes 前校验 record header、payload length、CRC32C、解压后大小和 CAS chunk hash。

## 元数据更新

`UpdateMetadata` 只替换文件的 string key/value metadata：

```text
content identity 不变
manifest_id 不变
file_hash 不变
chunk 不重写
```

VFS 的 mode、mtime、uid、gid、generation 等文件系统扩展字段保存在 file record 中，不写入 chunk。

## 删除与 GC

删除是元数据 tombstone：

```text
file.state = DELETED
manifest 无 active file 引用时 state = DELETED
```

物理空间由 GC 回收：

```text
1. 扫描 active files
2. 得到 live manifests
3. 得到 live chunks
4. 不可达 chunk 第一轮标记 GARBAGE_CANDIDATE
5. 达到确认轮数后标记 DELETED
6. segment 垃圾比例达到阈值时 compact
7. 无 live chunk 的 segment 到达删除延迟窗口后移除
```

默认 GC 配置：

```text
SafetyWindow: 24h
CandidateConfirmCycles: 2
SegmentDeleteDelay: 24h
CompactGarbageRatio: 0.6
```

## 损坏检查

`CheckObject` 校验单个 active object。

`Scrub` 扫描已存 chunk，可选校验 active file 拼接后的 hash。

检查内容：

```text
manifest chunk ref
chunk metadata
segment metadata
segment file
record header
payload length
CRC32C
zstd decompress
raw size
chunk hash
file hash
```

发现损坏后，相关 chunk 或 segment 会进入 `CORRUPT` 状态，读取和去重复用会避开这些数据。

## 故障恢复与观测

BlobFS core 只提供原始 struct API，不内置 HTTP、Prometheus 或 OpenTelemetry 封装：

```go
Health(ctx)
Stats(ctx)
Diagnose(ctx, opts)
Repair(ctx, opts)
```

`Health` 做轻量检查，用于判断 store 是否 open、metadata 是否加载、txlog 和数据目录是否可用、是否存在 corrupt 或 compacting 状态。它不扫描所有 segment。

`Health` 会在 metadata 锁下读取 txlog 和 checkpoint 状态；checkpoint 持久化失败会报告 `DEGRADED`，但只要 txlog 仍可写，不会误报为只读。

`Stats` 只从内存 metadata 聚合租户、inode、manifest、chunk、segment、字节和 GC 计数，不触发磁盘扫描。GC 计数是总次数和最后 epoch，不依赖无限增长的历史数组。

`Diagnose` 默认无副作用，可按选项扫描：

```text
CheckFiles:   检查 metadata 引用的 segment 文件是否存在
CheckOrphans: 扫 data/segments 中未被 metadata 引用的文件
CheckStaging: 扫 data/staging 残留临时文件
MaxIssues:    限制返回数量
```

`Repair` 只支持低风险动作，默认返回 dry-run 计划；只有 `Apply: true` 且未设置 `DryRun` 时才执行：

```text
CleanStaging:       删除 staging 临时文件
CleanOrphans:       删除 orphan segment 文件
ResetCompacting:    将残留 COMPACTING segment 恢复为 SEALED
MarkMissingCorrupt: segment 文件缺失时标记相关 segment/chunk 为 CORRUPT
```

高风险动作不在 `Repair` 中自动执行：不截断 txlog、不重建 manifest、不猜测缺失 chunk 内容。异常退出残留的 `LOCK` 会阻止 reopen；确认没有存活进程持有该 store 后，可显式调用 `RemoveStaleLock(baseDir)` 或 `RemoveFSStaleLock(fs, baseDir)` 删除 stale lock。

`Close` 会先停止后台 GC、等待已进入的写入/GC/修复操作结束，再 checkpoint 并关闭 metadata log，避免后台 goroutine 在 txlog 关闭后继续写入。

## VFS

`*Store` 实现 `afero.Fs`，同时提供 `TenantFS(tenantID) fs.FS`。

路径格式：

```text
tenant/path
```

VFS 支持：

```text
Create
Open
OpenFile
OpenFileContext
Mkdir
MkdirAll
Remove
RemoveAll
Rename
Stat
Chmod
Chown
Chtimes
TenantFS
```

普通文件默认清除执行位。设置 `AllowExecutableFiles` 后，VFS 文件可以保留执行位。目录、atime、mtime、uid、gid 存在 BlobFS 元数据中，tenant root 也使用对应的 root inode 元数据。

`RemoveAll` 的前台语义是 metadata detach：父目录项立即删除，路径立即不可见。子树内 inode、manifest 和 chunk 引用由后续 GC 扫描不可达 inode 后释放，避免在前台请求中递归遍历海量子树。

安全规则：

```text
tenant_id 只能包含字母、数字、_、-、.
object path 必须是相对路径
禁止空组件、.、..、NUL 和绝对路径
禁止跨 tenant rename
禁止把文件写到目录路径
禁止在文件路径下创建子项
目录非空时 Remove/Rename overwrite 会失败
```

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
```
