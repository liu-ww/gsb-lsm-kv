# lsmkv

一个**纯 Python 3 标准库**实现的嵌入式 LSM-Tree KV 存储引擎，零第三方依赖。
面向学习 LSM 原理：WAL、手写跳表 memtable、SSTable + 稀疏索引、size-tiered
后台压实、崩溃恢复都在清晰可读的代码里。

## 特性

- **写路径**：先追加 WAL 并 `fsync` 才确认提交；每条 WAL 记录带 CRC-32；
  memtable 是手写跳表（`lsmkv/skiplist.py`，无 sortedcontainers）；
  约 4MB 冻结并整体刷成有序 SSTable，随后删除对应 WAL 代。
- **SSTable**：按 key 有序、块级 CRC-32 校验、每 64 个 key 一个稀疏索引项；
  点查 = 稀疏索引二分定位块 + 块内二分。
- **删除**：tombstone 标记，WAL 与 SSTable 都持久化该标记。
- **读路径**：活跃 memtable → 冻结 memtable → 新段到旧段逐层查找；
  `get` 用 `None` 表示不存在、`b""` 表示存在的空值，二者严格区分。
- **压实**：单后台线程 size-tiered；多源归并按版本号取最新，tombstone 在
  无更老下层数据可遮蔽时物理丢弃；段快照列表 + 原子替换 MANIFEST，
  压实期间读不阻塞、写照常进入。
- **崩溃恢复**：启动重放 WAL；自动识别并只丢弃末尾被 `kill -9` 撕裂的残帧；
  已提交帧 CRC 损坏会报错而非静默丢数据。
- **线程安全**：单写锁 + 专用 flush/compaction 线程，支持多读者多写者并发。
- **二进制安全**：key/value 都是 `bytes`，空串、1KB、100KB、任意 `\x00` 均可。

## Python API

```python
from lsmkv import DB

with DB("./mydata") as db:          # sync_on_commit=True 默认每次提交 fsync
    db.put(b"key", b"value")
    db.put(b"empty", b"")
    assert db.get(b"key") == b"value"
    assert db.get(b"empty") == b""     # 存在的空值
    assert db.get(b"nope") is None     # 不存在
    db.delete(b"key")
    assert db.get(b"key") is None
    for k, v in db.scan(b"prefix:"):   # 惰性、有序、快照隔离
        ...
```

## CLI

```bash
python -m lsmkv <db_path> put <key> <value>
python -m lsmkv <db_path> get <key>
python -m lsmkv <db_path> delete <key>
python -m lsmkv <db_path> scan [prefix]
python -m lsmkv <db_path> bench        # 写 10 万随机 key(值~100B) 再读回校验，打印 QPS
```

`get` 命中时把原始 value 写到 stdout（找不到时退出码为 1）；
`scan` 每行输出 `key<TAB>value`。

## 测试

```bash
pytest                    # 有 pytest 环境时
python3 run_tests.py      # 纯标准库运行器（无 pytest 时跑同一批测试）
```

覆盖：基本读写删、覆盖写、删除/重写、空值/1KB/100KB、二进制安全、
10 万条多次刷盘后 scan 的有序完整、WAL 撕裂恢复、`os._exit(1)` 子进程崩溃恢复、
8 读 + 4 写并发压测、压实正确性与 CLI/bench 端到端。

## 代码结构

```
lsmkv/
  types.py      Entry（key/value/seq/tombstone）
  skiplist.py   手写跳表 memtable
  wal.py        WAL 帧编码、CRC、重放与撕裂识别
  sstable.py    SSTable 写/读、数据块校验、稀疏索引、footer
  manifest.py   段清单的原子读写
  db.py         LSM 引擎：写/读/flush/scan/压实/恢复
  __main__.py   CLI 与 bench
docs/FORMAT.md  三种磁盘格式的字节级布局
tests/          pytest 风格测试
run_tests.py    零依赖测试运行器
```
