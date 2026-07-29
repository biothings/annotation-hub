# NodeNorm Upload Memory Investigation

## Summary

The May 11, 2026 NodeNorm upload restart appears to be a memory pressure failure in the upload workflow, not a SQLite readonly failure.

The original SQLite error from an earlier run was real, but the May 11 retry did not show `sqlite3.OperationalError`, `attempt to write a readonly database`, or `database is locked`. The May 11 evidence points instead to the NodeNorm uploader consuming very large amounts of memory while processing file shards and returning identifier lists from worker processes to the parent process.

The first mitigation capped upload concurrency at 30 workers. That confirmed the previous 72-worker behavior was too aggressive, but the retry still climbed into the same memory range as the earlier failed run. The cap alone was not enough. The implemented patch stops returning giant identifier lists through `Future` objects and instead streams bounded identifier batches through multiprocessing queues into independently owned SQLite shards.

## What Happened

### Earlier Failure

An earlier failure raised:

```text
sqlite3.OperationalError: attempt to write a readonly database
```

That traceback occurred in `create_identifiers_table()` while running:

```python
DROP TABLE IF EXISTS identifiers
```

That is a separate issue. It may have been caused by an interrupted SQLite write, file system state, mount state, or rollback journal state. It was not reproduced in the May 11 retry logs.

### May 11 Retry

The May 11 upload started around:

```text
2026-05-11 20:26:12 PDT
```

The hub later restarted around:

```text
2026-05-11 21:25:42 PDT
```

The logs showed:

```text
May 11 21:25:45.589065 python[3989495]: Removing staled pid file '/data/annotator/run/3984963_BfeaC6hx.pickle'
May 11 21:25:45.992962 python[3989495]: Found stale datasource 'nodenorm', marking upload status as 'canceled'
```

This means a new hub process started and cleaned up a stale NodeNorm upload worker. The service log confirmed the restart, but did not contain the exact kernel or systemd reason for the old process exiting.

## Evidence Collected

### Service Restart Evidence

`systemctl status annotation_hub.service` showed the hub restarted:

```text
Active: active (running) since Mon 2026-05-11 21:25:42 PDT
Restart=on-failure
NRestarts=2
```

Later, after switching to the test branch and restarting the hub, the baseline became:

```text
Active: active (running) since Mon 2026-05-11 22:33:35 PDT
NRestarts=3
```

`NRestarts=3` became the baseline for the test run. The important signal is whether it increments during a run.

### Memory Pressure Evidence

The machine has:

```text
Mem: 376 GiB
Swap: 8 GiB
```

During the retry with the 30-worker cap, memory climbed rapidly:

```text
22:38 MemoryCurrent ~5 GiB
22:42 MemoryCurrent ~20 GiB
22:44 MemoryCurrent ~66 GiB
22:52 MemoryCurrent ~184 GiB
22:58 MemoryCurrent ~238 GiB
23:00 MemoryCurrent ~253 GiB
23:08 MemoryCurrent ~303 GiB
23:19 MemoryCurrent ~360 GiB
23:30 MemoryCurrent ~360 GiB, system used memory ~310 GiB
```

At the same time, `ps` showed large real Python RSS usage:

```text
Parent process RSS: ~25 to 34 GiB
Worker process RSS: often ~8 to 11 GiB each
Worker count: 30
```

This matters because `systemctl show ... MemoryCurrent` includes cgroup-charged page cache, so it is not equal to Python heap. However, `ps` RSS confirmed that Python processes themselves were also consuming large amounts of resident memory.

### Worker Cap Verification

After the first patch, the process cap was verified:

```bash
pgrep -P 3993444 | wc -l
```

Output:

```text
30
```

So the retry was not accidentally still using 72 workers. The cap worked, but 30 workers remained too memory-heavy for this code path.

### Progress Evidence

The uploader logs messages like:

```text
bulk write #[5000] in [0.4102]s | file Publication.txt subset progress: 99.451%
```

`#[5000]` is the batch size, not a batch sequence number.

The percentage is not global upload progress. It is calculated as:

```python
file_handle.tell() / offset_end
```

That means it is the current absolute byte position divided by the current shard end offset. It does not subtract `offset_start`, so it is not a reliable per-shard percentage either.

The better progress signal is parent task completion:

```text
Task 89 completed | Update 729236 identifiers | Total identifiers 87331569
```

There are approximately 251 upload shard tasks, based on `NODENORM_UPLOAD_CHUNKS`. At task 89, the upload was only about 36 percent complete by shard count, yet memory was already near the earlier peak. That strongly supports the memory retention diagnosis.

## Root Cause in the Former Design

The former uploader created very large in-memory identifier lists in worker processes, returned those lists to the parent process, then duplicated them again before SQLite insertion.

The relevant flow is in `plugins/nodenorm/worker.py`.

### Worker-Side Identifier Accumulation

Each process worker creates a list:

```python
identifiers = []
```

For every document identifier, it appends a string:

```python
for identifier in doc["identifiers"]:
    identifiers.append(identifier["i"])
```

At the end of the worker shard, it returns the whole list:

```python
return identifiers
```

Some completed tasks returned millions of identifiers:

```text
Update 2647970 identifiers
Update 2648292 identifiers
Update 2647015 identifiers
```

With 30 concurrent workers, this means many multi-million-element lists can exist at the same time.

### ProcessPool Result Transfer

`ProcessPoolExecutor` must serialize returned values from child processes back to the parent process. Returning a multi-million-string list can create several memory copies:

```text
worker list
serialized result buffer
parent-side deserialized list
Future result reference
```

This is expensive even before the parent writes anything to SQLite.

### Future Result Retention

The parent keeps every future in a list:

```python
process_futures = []
...
process_futures.append(future)
```

It then iterates completed futures:

```python
for index, future in enumerate(concurrent.futures.as_completed(process_futures)):
    identifiers = future.result()
    update_identifier_collection(data_folder, identifiers)
    del identifiers
```

The local `del identifiers` does not necessarily free the returned list, because the `Future` object can still retain the result. Since the `process_futures` list is kept until the executor completes, completed task results may remain alive far longer than expected.

This explains why the parent process RSS grew steadily during the run.

### Parent-Side SQLite Materialization

Before writing to SQLite, the parent duplicates the identifier data into a new list of dictionaries:

```python
identifier_information = [{"identifier": identifier} for identifier in identifiers]
cursor.executemany(upsert_statement, identifier_information)
```

For millions of identifiers, this creates another large allocation. This is avoidable because SQLite can consume an iterator or smaller batches.

## Why The 30-Worker Patch Was Not Enough

The first mitigation changed both heavy executor pools from CPU count to a constant:

```python
NODENORM_WORKER_COUNT = 30
```

That reduced concurrency from about 72 logical CPUs to 30 workers. It confirmed that the previous configuration was too aggressive.

However, the retry still reached:

```text
MemoryCurrent ~360 GiB
Parent RSS ~34 GiB
Worker RSS ~8 to 11 GiB each
```

This means the issue is not only too many workers. The data transfer model is also wrong for this workload. Returning giant identifier lists through process futures is the main memory multiplier.

## Implemented Patch

The implemented patch keeps Mongo upload behavior the same but changes how identifiers move from worker processes into SQLite:

```text
worker collects at most 100,000 identifiers
worker routes them deterministically across 8 shards using CRC32
worker places each nonempty shard batch on its bounded multiprocessing queue
one parent writer thread owns each SQLite shard and drains its queue
writer commits up to 8 queued batches at a time
worker returns only an integer identifier count
parent discards each completed Future
duplicate cleanup streams results from every SQLite shard
```

Each queue is bounded to 60 pending batches, so backpressure prevents workers from building an unbounded parent-side backlog. A dedicated writer thread and SQLite connection own each shard; workers never share SQLite connections. The shard databases use WAL mode, `synchronous=NORMAL`, `WITHOUT ROWID` identifier tables, and partial duplicate indexes.

All multiprocessing primitives and the process pool use the same explicit `spawn` context. This avoids forking the parent after its SQLite writer threads have started and keeps process behavior consistent across platforms.

`SQLITE_TMPDIR` controls SQLite's own temporary files; it is not used to transfer identifiers between tasks. An operator-provided value is preserved. Otherwise the uploader uses `sqlite_tmp` under `DATA_ARCHIVE_ROOT`, creates the directory before opening SQLite, and verifies that it is writable.

The current implementation retains the 30-worker cap. The queue and shard design addresses the former result-retention multiplier, but the next full upload still needs memory and throughput monitoring before increasing concurrency.

## Expected Impact

The patch reduces memory pressure in three places:

1. Workers retain only a bounded identifier batch rather than a full shard list.
2. The parent receives bounded queue traffic rather than multi-million-string `Future` results.
3. SQLite consumes identifier iterators directly without constructing a second giant list of dictionaries.

Expected runtime tradeoff:

```text
Memory: bounded by worker, batch, and queue limits
IPC: more frequent, bounded queue transfers
Disk I/O: parallel writes across 8 SQLite shards
Runtime: dependent on Mongo and SQLite throughput, but without unbounded result retention
```

## Verification Plan

Before rerunning:

```bash
rg -n 'get_context\("spawn"\)|mp_context=|SQLITE_TMPDIR|_queue_identifier_batch|_write_identifier_batches' plugins/nodenorm/worker.py
python -m py_compile plugins/nodenorm/worker.py
```

Restart the hub after switching branches:

```bash
curl -X PUT http://localhost:19280/restart
systemctl status annotation_hub.service --no-pager
```

Confirm the new process is running:

```bash
systemctl show annotation_hub.service -p MainPID -p NRestarts -p MemoryCurrent -p MemoryPeak
```

During upload, monitor system memory, Python RSS, and service restart count:

```bash
watch -n 10 'date; free -h; echo; ps -o pid,ppid,rss,vsz,comm -C python --sort=-rss | head -20; echo; systemctl show annotation_hub.service -p MemoryCurrent -p MemoryPeak -p MemorySwapCurrent -p MemorySwapPeak -p TasksCurrent -p NRestarts'
```

Confirm worker cap:

```bash
pgrep -P "<upload-parent-pid>" | wc -l
```

Track parent task completions:

```bash
journalctl -u annotation_hub.service --since "2026-05-11 22:38:00" -o cat | grep 'Task .* completed | Update' | tail -20
```

Success criteria:

```text
NRestarts does not increment
Parent RSS does not grow monotonically into tens of GiB
Worker RSS remains bounded
System swap does not increase because of this service
Upload reaches duplicate cleanup and final index creation
```

## Operational Recommendation

Do not run the former result-return design again. Capping worker count alone did not remove its primary memory multiplier.

Deploy the bounded queue and sharded SQLite implementation, restart the hub, and monitor the next full upload at the current 30-worker cap. If memory or disk pressure is still unacceptable, lower the cap before retrying; only increase it after a stable full run.
