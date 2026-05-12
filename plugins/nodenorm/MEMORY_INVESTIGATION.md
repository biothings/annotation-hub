# NodeNorm Upload Memory Investigation

## Summary

The May 11, 2026 NodeNorm upload restart appears to be a memory pressure failure in the upload workflow, not a SQLite readonly failure.

The original SQLite error from an earlier run was real, but the May 11 retry did not show `sqlite3.OperationalError`, `attempt to write a readonly database`, or `database is locked`. The May 11 evidence points instead to the NodeNorm uploader consuming very large amounts of memory while processing file shards and returning identifier lists from worker processes to the parent process.

The first mitigation capped upload concurrency at 30 workers. That confirmed the previous 72-worker behavior was too aggressive, but the retry still climbed into the same memory range as the earlier failed run. The cap alone is not enough. The proposed patch is to stop returning giant identifier lists through `Future` objects and instead stream identifiers through per-task temporary files into SQLite.

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

## Root Cause

The uploader creates very large in-memory identifier lists in worker processes, returns those lists to the parent process, then duplicates them again before SQLite insertion.

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

## Proposed Patch

The proposed patch keeps Mongo upload behavior the same, but changes how identifiers move from worker processes to the parent process.

Instead of returning:

```python
list[str]
```

each worker should write identifiers to a temporary file and return:

```python
(identifier_file_path, identifier_count)
```

The parent should stream the file into SQLite in bounded batches, then delete the temporary file.

### New Flow

Current flow:

```text
worker builds giant list
worker returns giant list
ProcessPool serializes giant list
parent receives giant list
Future retains giant list
parent builds second list of dicts
SQLite writes second list
```

Proposed flow:

```text
worker writes identifiers line by line to temp file
worker returns small tuple: file path and count
parent streams temp file into SQLite in batches
parent deletes temp file
Future retains only a small tuple
```

### Patch Outline

Create a per-run identifier directory under the data folder:

```python
identifier_chunk_dir = Path(data_folder).joinpath(".identifier_chunks")
shutil.rmtree(identifier_chunk_dir, ignore_errors=True)
identifier_chunk_dir.mkdir()
```

When submitting each task, assign an output file:

```python
for index, task in enumerate(_build_offset_tasks(data_folder, collection_name)):
    task["identifier_output_file"] = identifier_chunk_dir / f"{index}.txt"
    future = executor.submit(subset_upload_worker, **task)
```

Change the worker signature:

```python
def subset_upload_worker(..., identifier_output_file: Union[str, Path]) -> tuple[str, int]:
```

Inside the worker, stream identifiers to disk:

```python
identifier_count = 0

with open(identifier_output_file, "w", encoding="utf-8") as identifier_handle:
    ...
    for identifier in doc["identifiers"]:
        identifier_handle.write(identifier["i"])
        identifier_handle.write("\n")
        identifier_count += 1
        identifier["c"] = {"gp": None, "dc": None}

return str(identifier_output_file), identifier_count
```

In the parent, ingest and remove the file:

```python
identifier_file, identifier_count = future.result()
update_identifier_collection_from_file(data_folder, identifier_file)
Path(identifier_file).unlink(missing_ok=True)
total_document_count += identifier_count
```

Replace list-of-dicts SQLite insertion with chunked streaming:

```python
def update_identifier_collection_from_file(data_folder, identifier_file):
    identifier_database = Path(data_folder).joinpath(IDENTIFIER_LOOKUP_DATABASE)
    identifier_connection = sqlite3.connect(str(identifier_database))
    cursor = identifier_connection.cursor()

    upsert_statement = (
        "INSERT INTO identifiers(identifier) "
        "VALUES(?) "
        "ON CONFLICT(identifier) "
        "DO UPDATE SET count=count+1;"
    )

    with open(identifier_file, encoding="utf-8") as handle:
        while True:
            batch = tuple(
                (line.rstrip("\n"),)
                for _, line in zip(range(50000), handle)
            )
            if not batch:
                break
            cursor.executemany(upsert_statement, batch)
            identifier_connection.commit()

    identifier_connection.close()
```

This keeps peak memory bounded by the batch size rather than by the full shard identifier count.

### Optional Additional Improvement

After the temp-file patch, completed futures will retain only small tuples. That makes future retention much less dangerous.

Still, it is cleaner to avoid storing all futures indefinitely. The parent can remove completed futures or use a bounded submission pattern. This is useful, but it is less important once the returned result is small.

## Recommended Worker Count

The 30-worker cap was useful for testing, but it still allowed very high memory usage. After the temp-file patch, a safer next test should use:

```text
10 to 15 workers
```

Once the memory profile is stable, increase gradually if needed.

Recommended behavior:

```text
Default: 10 or 15
Configurable via environment variable
Avoid os.cpu_count() for this workload
```

Example:

```python
NODENORM_WORKER_COUNT = int(os.getenv("NODENORM_WORKER_COUNT", "15"))
```

## Expected Impact

The patch should reduce memory pressure in three places:

1. Workers no longer retain all identifiers as a Python list for return.
2. The parent no longer receives and stores multi-million-string lists.
3. SQLite insertion no longer builds a second giant list of dictionaries.

Expected runtime tradeoff:

```text
Memory: much lower and flatter
Disk I/O: slightly higher due to temporary identifier files
Runtime: possibly slightly slower, but much less likely to restart the hub
```

The disk tradeoff is acceptable because the current memory behavior can restart the service and cancel the upload.

## Verification Plan

Before rerunning:

```bash
grep -n "NODENORM_WORKER_COUNT\|identifier_output_file\|update_identifier_collection_from_file" plugins/nodenorm/worker.py
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

Do not continue full NodeNorm uploads with the current result-return design. Capping worker count helps, but it does not remove the primary memory multiplier.

Apply the temp-file streaming patch first, restart the hub, and retry with a lower worker count such as 10 or 15. If that run is stable, increase the cap gradually.

