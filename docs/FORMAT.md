# lsmkv 磁盘格式

本文档用字节级布局描述 lsmkv 的三种磁盘结构：WAL 记录、SSTable 段文件、
稀疏索引。所有整数均为**小端（little-endian）**，`u32` = 4 字节无符号整数，
`u64` = 8 字节无符号整数。校验和一律使用 `zlib.crc32`。

数据库目录下的文件命名：

```
<db_path>/
  wal-000042.log            # 一个 memtable 对应一个 WAL 文件
  tier0-seg000043.sst       # tier<t>-seg<id>.sst，id 全局单调递增
  tier1-seg000047.sst
```

---

## 1. WAL 记录

WAL 是记录的纯追加序列。每条记录布局如下（总头长 21 字节）：

```
偏移   长度        字段        含义
0      4           crc32       对记录第 4 字节到记录末尾的所有字节做 crc32
4      4           key_len     u32，key 的字节数
8      4           value_len   u32，value 的字节数（tombstone 时恒为 0）
12     8           seqno       u64，全局单调递增的写入序号
20     1           flags       bit0 = tombstone（1 表示这是一条删除标记）
21     key_len     key         原始 key 字节
21+kl  value_len   value       原始 value 字节
```

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                            crc32                              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           key_len                             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          value_len                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      seqno (u64, 8B)                          |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     flags     |  key bytes ...  |  value bytes ...            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

提交语义：`put`/`delete` 只有在记录完整写入 WAL 并 `fsync` 之后才返回。

**崩溃恢复 / torn write**：恢复时从头顺序解码。遇到以下任一情况即认为
文件尾部被撕坏（例如 `kill -9` 时没写完的最后一条记录），停止重放并把
文件截断到最后一条完整记录的末尾——只丢弃尾部那一条残缺/坏掉的记录，
它之前的完整记录一条不丢：

1. 读不满 21 字节头（torn header）；
2. 读不满 `key_len + value_len` 字节负载（torn payload）；
3. crc32 校验失败（corrupt record）。

memtable 刷盘成 SSTable 后，对应的 WAL 文件被删除。

---

## 2. SSTable 段文件

段文件由四部分组成：数据块序列、稀疏索引、索引校验、定长 footer。

```
+-------------------+  <-- 0
| data block 0      |
| block crc32 (4B)  |
+-------------------+
| data block 1      |
| block crc32 (4B)  |
+-------------------+
| ...               |
+-------------------+
| index             |
| index crc32 (4B)  |
+-------------------+
| footer (40B)      |  <-- 文件末尾，定长，从尾部 seek 即可找到
+-------------------+
```

### 2.1 数据块（data block）

每个数据块最多装 `INDEX_EVERY = 64` 条 entry，块内 entry 按 key 升序。
块级校验：每个块原始字节之后紧跟 4 字节 crc32（对块内容计算，不含
crc 自身）。读取任何块之前先校验，失败即报 `CorruptionError`。

单条 entry 布局（与 WAL 记录体完全一致，21 字节头）：

```
偏移   长度        字段        含义
0      4           key_len     u32
4      4           value_len   u32
8      8           seqno       u64
16     1           flags       bit0 = tombstone
17     key_len     key
17+kl  value_len   value
```

### 2.2 稀疏索引（index）

每 64 个 key 一个索引项（即每个数据块一项），索引项的 key 是该块的
**第一个 key**。布局：

```
偏移   长度        字段            含义
0      4           count           u32，索引项个数
4      ...         index entries   见下，共 count 项
...    4           crc32           对之前所有索引字节的校验
```

单个索引项（变长）：

```
偏移   长度        字段            含义
0      4           key_len         u32
4      8           block_offset    u64，该块在文件内的起始偏移
12     4           block_len       u32，块长度（含块尾 4B crc）
16     key_len     first_key       该块第一个 key
```

点查流程：对 `first_key` 数组二分，找到「最后一个小于等于目标 key」的
索引项 → 按 `block_offset`/`block_len` 读出整块并校验 crc → 块内再次
二分定位 entry。

### 2.3 Footer（定长 40 字节，位于文件末尾）

```
偏移   长度        字段            含义
0      8           index_offset    u64，索引区起始偏移
8      8           index_len       u64，索引区长度（含索引自身 4B crc）
16     8           entry_count     u64，段内 entry 总数
24     8           max_seqno       u64，段内最大 seqno（恢复时用于续号）
32     8           magic           固定字节 "LSMKVST1"
```

---

## 3. 层级与压实

- 刷盘产生的新段进入 tier 0；某 tier 段数达到 `TIER_FANOUT = 4` 时，
  后台线程把**该 tier 全部段**归并成下一 tier 的一个新段
  （size-tiered，整层合并保证「浅层数据一定比深层新」）。
- 归并时对每个 key 取 seqno 最大的版本；被 tombstone 覆盖的旧版本丢弃。
- 仅当被合并的 tier 之下已无任何更老的段时，tombstone 本身才可丢弃。
- 压实期间读写不阻塞：读路径在锁内对段列表做快照，压实完成后原子替换
  tier 列表，再 `unlink` 旧段文件（已打开的文件描述符不受影响）。

## 4. 崩溃一致性保证

- 单条写入：先 WAL + fsync，再进 memtable —— 已确认的写入必然可恢复。
- 段文件：先完整写完（含 footer）并 fsync，再原子地加入段清单并删除旧
  WAL / 旧段；崩溃只会留下未被引用的孤儿文件，不影响正确性。
- 恢复顺序：扫描目录重建 tier 结构 → 按 id 顺序重放所有 WAL →
  `seqno` 从全库最大值续起。
