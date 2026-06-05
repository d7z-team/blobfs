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

## 文件分类

配置：

```text
LargeFileThreshold: 64 MiB
```

当前写入策略：

```text
size <= LargeFileThreshold:
  whole-file SHA-256
  single chunk

size > LargeFileThreshold:
  FastCDC chunking
  per-chunk SHA-256
```

小文件和中等文件都表现为单 chunk manifest。大文件使用 FastCDC，默认参数：

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
    blobfs.json
    LOCK
  data/
    segments/
      0000/
        0000/
          0000000000000001.blob
  tmp/
    write-sessions/
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

当前元数据存储为原子替换的 JSON 文件：

```text
meta/blobfs.json
```

核心记录：

```text
files:
  file_id
  tenant_id
  path
  parent_path
  name
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
  tenant_id + parent_path -> child_name -> file key

manifests:
  manifest_id
  tenant_id
  file_size
  file_hash
  chunk_count
  chunking_type
  chunks
  state
  created_at
  last_live_at
  deleted_at

chunks:
  chunk_id
  tenant_id
  raw_size
  stored_size
  state
  segment_id
  segment_offset
  segment_length
  checksum_crc32c
  compression
  gc fields
  corruption fields

segments:
  segment_id
  relative_path
  write_offset
  total_bytes
  live_bytes_estimate
  garbage_bytes_estimate
  state
  timestamps
```

状态：

```text
file:      ACTIVE, DELETED
file kind: FILE, DIR, SYMLINK
manifest:  ACTIVE, DELETED
chunk:     WRITING, ACTIVE, GARBAGE_CANDIDATE, DELETING, DELETED, CORRUPT
segment:   OPEN, SEALED, COMPACTING, COMPACTED, DELETED, CORRUPT
```

目录是显式元数据记录。BlobFS 不从 `a/b.txt` 自动合成 `a`，所以写入嵌套 object 前必须先创建父目录。目录列表只依赖 `dir_entries`，不扫描全量文件名。

## 写入

```text
1. 校验 context、tenant、path 和 reader
2. 校验父目录、文件大小上限和路径长度限制
3. 读取输入
4. 计算 scoped file hash
5. 查找可复用 manifest
6. 写入缺失 chunk 到 segment
7. 写 chunk metadata
8. 写 manifest
9. 写 file record
10. 保存 metadata
```

顺序要求：

```text
segment payload -> chunk metadata -> manifest -> file record
```

这样 file record 可见时，它引用的 chunk 已经可读。

VFS 写入使用临时 write session：

```text
OpenFile writable
  -> 创建 tmp/write-sessions/session-*.tmp
  -> 复制旧内容或从空文件开始
  -> Write/WriteAt/Truncate 修改 session
  -> Sync/Close 带 base generation 提交
```

如果打开句柄后同一路径被其他写入修改，提交会返回 `ErrConflict`。新建句柄在关闭前如果同路径已经被创建，也会返回 `ErrConflict`。

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

读取会拒绝不可读的 chunk 和 segment 状态。

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
5. 第二轮仍不可达标记 DELETING
6. segment 垃圾比例达到阈值时 compact
7. compact 后的 segment 到达延迟窗口后移除
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

普通文件默认清除执行位。设置 `AllowExecutableFiles` 后，VFS 文件可以保留执行位。目录、mtime、uid、gid 存在 BlobFS 元数据中。

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
    LargeFileThreshold   int64
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
LargeFileThreshold: 64 MiB
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
