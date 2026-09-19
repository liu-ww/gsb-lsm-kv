# lsmkv

嵌入式 LSM-Tree KV 存储引擎，纯 Python 3 标准库实现（零第三方依赖），
用于学习 LSM 原理。

## 特性

- **写路径**：WAL 追加 + fsync 后才算提交，每条记录带 crc32；手写跳表
  memtable，4MB 刷盘为 SSTable 并截断对应 WAL。
- **SSTable**：key 有序存储，每 64 个 key 一条稀疏索引，块级 crc32 校验，
  删除以 tombstone 持久化。
- **读路径**：memtable → immutable memtable → tier 0 → tier 1 …，
  稀疏索引定位块 + 块内二分；`get` 能区分「不存在」（`None`）和
  「空值」（`b""`）。
- **压实**：后台单线程 size-tiered，整层合并；压实期间读写不阻塞。
- **崩溃恢复**：启动重放 WAL；尾部被撕坏的记录（torn write）会被识别并
  截断，之前的完整记录一条不丢；全引擎线程安全。

磁盘格式的字节级布局见 [docs/FORMAT.md](docs/FORMAT.md)。

## Python API

```python
from lsmkv import DB

db = DB("/path/to/db")          # 线程安全
db.put(b"key", b"value")
db.get(b"key")                  # -> b"value"；不存在/已删除 -> None
db.delete(b"key")
for k, v in db.scan(b"ke"):     # 惰性迭代器，按 key 有序
    ...
db.close()
```

key 和 value 都是 bytes（str 会自动按 UTF-8 编码），二进制安全，
空串 / 1KB / 100KB 的值都没问题。

## CLI

```bash
python -m lsmkv <db_path> put <key> <value>
python -m lsmkv <db_path> get <key>
python -m lsmkv <db_path> delete <key>
python -m lsmkv <db_path> scan <prefix>
python -m lsmkv <db_path> bench          # 写 10 万条随机 key 并读回校验，打印 QPS
```

## 测试

```bash
pytest tests/                 # 如果装了 pytest
python tests/run_tests.py     # 纯标准库 runner，效果相同
```

覆盖：基本读写删、覆盖写、删后重写、空值/1KB/100KB、10 万条触发多次刷盘后
scan 有序性完整性、WAL torn write 恢复、子进程 `os._exit(1)` 崩溃恢复、
4 写 + 8 读线程并发压测（`LSMKV_STRESS_SECS=30` 可跑 30 秒完整版）。
