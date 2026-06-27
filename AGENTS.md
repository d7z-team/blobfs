# AGENTS.md

## 工作原则

- 先阅读现有代码和文档，再修改；不要基于猜测重写实现。
- 保持改动直接、简单、可维护；不要引入过度抽象。
- 不要盲目新增依赖；确需新增依赖时先说明理由。
- 不要回退用户已有改动，除非用户明确要求。
- 删除代码前先确认它确实已经无用或过时。
- 不要编写过度的抽象，注意清理大块的重复代码
- 不要封装过多单次使用的小函数，清理已经没作用的测试
- 对于测试中多次使用的可抽取 helper 来减少代码量
- 优化大的代码块结构，清理看起来特别值得优化的代码
- 错误语义完全等价时直接复用 Go 标准错误

## 开发命令

- 格式化：`make fmt`
- 单元测试：`make test`
- 竞态测试：`make race`
- 静态检查：`make lint`
- 覆盖率：`make cover`

## 测试要求

- 行为变更需要补充或更新相应测试。
- 优先覆盖热点路径、边界参数、错误路径和故障恢复场景。
- 对热点代码覆盖测试，覆盖边界测试，增加冒烟测试，增加混沌测试
- 完成代码修改后至少运行 `make lint test`。

## 元数据版本迁移架构

### 原则

1. **旧版本隔离**：每个历史版本的结构体和迁移逻辑放在独立的 `vN_store_meta.go` 文件，冻结不随主代码演化。
2. **链式升级**：`v1 → v2 → v3 → ...` 每步一个迁移函数，按序执行。
3. **幂等迁移**：所有迁移函数支持崩溃重试，检测已迁移部分并跳过。
4. **版本真源**：`SUPER0/SUPER1` 的 `format_version` 字段是唯一的版本真相来源。
5. **记录类型兼容**：`inodeRecord` 等记录类型新增字段必须使用 `omitempty` 标签。若需破坏性变更，在对应 `vN_store_meta.go` 中冻结旧类型拷贝。

### 文件布局

```
metadata.go          ← 当前版本核心逻辑
v1_store_meta.go     ← v1 结构体 + v1→v2 迁移函数（冻结）
v1_store_meta_test.go← v1 迁移专用测试
```

### 添加新版本 (v3 为例)

1. 新建 `v2_store_meta.go`，冻结 v2 `metadata` 结构体
2. 实现 `migrateV2ToV3(fs afero.Fs, metaDir string) error`
3. 在 `migrationChain` 注册：`2: migrateV2ToV3`
4. 更新 `metaFormatVersion` 常量
5. 若 `inodeRecord` 等记录类型需要破坏性变更，在此文件中冻结旧类型拷贝
6. 新增 `v2_store_meta_test.go` 覆盖迁移测试
7. 放宽 `decodeMetaSuperBlock` 版本上限（自动随 `metaFormatVersion` 提升）
