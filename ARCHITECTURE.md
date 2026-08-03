# pget Technical Architecture

Status: Final

## 1. System overview

`pget` is a single Go command-line executable that downloads HTTP, HTTPS, FTP, and FTPS objects.

It follows Wget's sequential URL and output model while adding parallel fixed-size retrieval inside the currently active object. The downloader has two output paths:

```text
Protocol adapter and range scheduler
├── Ordered stream sink -> stdout or another non-seekable writer
└── Positional file sink -> destination file plus `.pget` sidecar
```

The ordered stream sink downloads ranges concurrently, buffers completed ranges in memory, and emits bytes strictly in source order. The positional file sink writes completed ranges directly to their final offsets and records completed ranges in a resumable sidecar.

Only one URL is active at a time. There is no cross-URL download scheduler.

## 2. Architecture goals

- One deployable Go executable
- Wget-compatible option names and user-visible semantics where implemented
- Parallel retrieval of one remote object over several connections
- Strictly ordered streamed output
- Hard memory bound for stream chunk payloads
- Resumable out-of-order file downloads
- Safe representation validation across ranges
- Automatic fallback to sequential transfer
- Gradual adaptation to connection and server limits
- Direct HTTP, HTTPS, FTP, and FTPS support
- Predictable behavior suitable for shell pipelines and automation

## 3. Non-goals

- Recursive retrieval or mirroring
- HTML parsing or link conversion
- Cookie storage
- Robots processing
- Concurrent retrieval of separate URLs
- A daemon, RPC interface, or persistent queue
- Automatic chunk-size tuning
- Adaptive performance presets
- HTTP/2 range multiplexing
- Temporary-file spill for ordered stream chunks
- TLS client certificates or mutual TLS
- FTP directory traversal or wildcard expansion

## 4. Key architecture decisions

### 4.1 Go implementation

Use Go because it provides:

- Simple concurrent network programming
- Reliable cancellation through contexts
- Direct positional file writes
- A single executable deployment model
- Mature HTTP and TLS support
- Straightforward Unix process detachment

Use a small maintained Go FTP/FTPS dependency rather than implementing FTP control and data protocols from scratch. Pin the dependency version. It must support passive mode, `SIZE`, `MDTM`, `REST`, explicit TLS, implicit TLS, configurable timeouts, and cancellation or forced connection close.

### 4.2 Fixed chunk size

Chunks are fixed for the lifetime of one remote object.

Defaults:

```text
requested connections: 8
split size:             8 MiB
stream buffer limit:    128 MiB
```

There is no automatic chunk resizing. The final chunk may be smaller.

### 4.3 Sequential URLs

The job runner processes URLs in input order. Internal chunk concurrency applies only to the active URL.

### 4.4 Separate stream and file sinks

The stream sink requires reordering and bounded memory. The file sink can use positional writes and therefore does not need to retain complete out-of-order chunks in memory.

This split is intentional. Forcing both modes through one ordered writer would waste memory and lose arbitrary-piece resume in file mode.

### 4.5 Wget behavior over aria2 behavior

Wget naming, URL order, output naming, concatenation, background mode, timestamping, and continue semantics take precedence.

The aria2-inspired parts are limited to:

- Several connections for one object
- Fixed-size pieces
- A resumable sidecar with a completion bitmap
- Requested concurrency as a maximum rather than a guarantee

## 5. Process structure

A single foreground process contains all download components.

In background mode, a short-lived parent starts one detached child running the same executable and arguments with an internal child marker. The child performs the normal architecture described below.

There are no helper daemons.

## 6. Major components

### 6.1 CLI parser

Responsibilities:

- Parse Wget-compatible short and long options
- Preserve URL order
- Read additional URLs from `-i`
- Validate incompatible options
- Resolve defaults
- Build an immutable execution plan

The parser must support options interspersed with URLs and combined argument-free short flags. Go's standard `flag` package is insufficient by itself for full Wget-style parsing; use a small GNU-compatible parser or a focused internal parser with comprehensive tests.

### 6.2 Execution planner

Produces:

```text
ExecutionPlan
  URLs in exact order
  output mode
  filename policy
  logging policy
  retry policy
  timeout policy
  parallel settings
  timestamp policy
  background policy
```

The planner rejects invalid combinations before creating or modifying destination files.

### 6.3 Job runner

Processes one `DownloadJob` at a time.

Responsibilities:

- Resolve destination naming
- Apply `-nc`, `-N`, and `-c`
- Probe remote metadata and parallel capability
- Select parallel or sequential retrieval
- Create the correct sink
- Run the object scheduler
- Finalize output and timestamps
- Continue or stop according to normal-file versus `-O` semantics

### 6.4 Protocol adapters

Expose a common object interface while retaining protocol-specific validation:

```text
Probe(ctx, URL, request options) -> ObjectMetadata
OpenRange(ctx, worker, start, length, validator) -> RangeReader
OpenSequential(ctx, offset) -> Reader
```

Adapters:

- HTTP/HTTPS adapter
- FTP/FTPS adapter

### 6.5 Chunk planner

Given object size `L` and split size `S`, produce:

```text
chunk count = ceil(L / S)
chunk i start = i * S
chunk i length = min(S, L - start)
```

Chunk identity is the zero-based index plus exact start and length.

The planner never changes `S` after scheduling begins.

### 6.6 Scheduler

Responsibilities:

- Maintain requested and effective concurrency
- Assign chunks continuously, without batches
- Track active, completed, retriable, and suppressed work
- Enforce stream memory reservations
- Handle per-chunk retries
- Reduce and later restore connection count
- Start at most one speculative duplicate
- Cancel all work on fatal job failure

### 6.7 Connection controller

Tracks:

```text
requested concurrency
memory-limited concurrency
current effective concurrency
active workers
suppressed worker slots
cooldown deadline
recovery backoff
recent successful chunk statistics
```

It is separate from retry policy because a failed chunk and an unavailable connection slot are different events.

### 6.8 Ordered stream sink

Responsibilities:

- Reserve chunk payload memory before download
- Receive completed chunk buffers
- Hold them by chunk index
- Write only the next expected chunk
- Release memory after write
- Expose frontier progress to the speculative duplicate controller
- Cancel on destination write failure

### 6.9 Positional file sink

Responsibilities:

- Create or open the destination
- Write each completed range using `WriteAt`
- Validate exact byte count
- Update the in-memory completion bitmap
- Checkpoint durable data before sidecar state
- Finalize file length and modification time

Workers should stream network data into a moderate reusable I/O buffer and positional writes. File mode does not need to allocate one full split-size buffer per worker.

### 6.10 Sidecar manager

Responsibilities:

- Create and validate `.pget` state
- Store representation identity and completed-piece bitmap
- Atomically checkpoint state
- Reject incompatible resume attempts
- Remove the sidecar only after durable successful completion

### 6.11 Progress and logger

Responsibilities:

- Keep binary output separate from diagnostics
- Render TTY and non-TTY progress
- Report requested and effective connections
- Report fallback, retry, reduction, recovery, and hedge events in verbose mode
- Redact credentials and authorization data

## 7. Core data model

### 7.1 DownloadJob

```text
DownloadJob
  source URL
  sanitized display URL
  destination mode
  destination path or stdout
  concatenated output base offset
  retry and timeout policy
  protocol request settings
  requested connections
  split size
  stream buffer size
  continue mode
  timestamp mode
```

### 7.2 ObjectMetadata

```text
ObjectMetadata
  original URL identity hash
  final URL identity hash
  sanitized final display URL
  protocol
  total size, if known
  remote modification time, if known
  strong ETag, if available
  Last-Modified value, if usable
  range/restart capability
  suggested filename
```

Credential-bearing URLs and request headers must not be persisted or logged. Store hashes for exact identity matching and sanitized display forms for diagnostics.

### 7.3 Chunk

```text
Chunk
  index
  start offset
  length
  state
  attempt count
  assigned worker
  start time
  bytes received
  last progress time
  hedge-used flag
```

States:

```text
pending
active
completed
written
retry-wait
failed
```

### 7.4 Worker statistics

```text
WorkerStats
  slot ID
  protocol connection state
  useful bytes transferred
  successful chunk count
  admission failure count
  recent throughput
  recent chunk durations
  suppression deadline
```

### 7.5 Sidecar state

Use versioned JSON for v1. A base64-encoded bitset is compact enough for large files while keeping implementation and recovery inspection simple.

Conceptual structure:

```json
{
  "version": 1,
  "destination": "image.iso",
  "url_list_hash": "sha256:...",
  "created_at": "2026-08-03T21:00:00Z",
  "updated_at": "2026-08-03T21:03:00Z",
  "items": [
    {
      "source_url_hash": "sha256:...",
      "final_url_hash": "sha256:...",
      "display_url": "https://example.org/image.iso",
      "output_offset": 0,
      "length": 4294967296,
      "split_size": 8388608,
      "etag": "\"abc123\"",
      "last_modified": "Mon, 03 Aug 2026 12:00:00 GMT",
      "remote_mtime_unix": 1785758400,
      "completed_bitmap": "base64:...",
      "complete": false
    }
  ]
}
```

Do not store:

- Passwords
- URL userinfo
- Authorization headers
- Cookies
- TLS private material

For multiple independent output files, each file has its own sidecar. For `-O FILE` with multiple URLs, one sidecar contains the ordered item list and each item's output base offset.

## 8. HTTP and HTTPS adapter

### 8.1 Probe

Issue:

```http
GET /object HTTP/1.1
Range: bytes=0-0
Accept-Encoding: identity
```

The probe must follow allowed redirects and record the final URL identity.

Parallel mode requires:

- Valid `206 Partial Content`
- Exact `Content-Range` for byte zero
- Known total size
- A usable representation validator

A usable validator is:

1. A strong ETag, preferred; or
2. A stable `Last-Modified` value combined with exact total size

Without a usable validator, fall back to a sequential request.

Do not rely only on `HEAD` or `Accept-Ranges`.

### 8.2 Worker connections

Use separate HTTP/1.1 transports for worker slots so each active worker normally owns one persistent TCP/TLS connection.

Per-worker transport requirements:

```text
HTTP/2 disabled
transparent compression disabled
maximum one active connection for that worker transport
keep-alive enabled
shared TLS and proxy settings
```

Workers reuse their connection across range requests when the server permits it.

The TLS and TCP setup cost is therefore normally paid once per worker, not once per chunk. Chunk size still affects request/response latency, retry granularity, load balancing, and reorder memory.

### 8.3 Range request

For a strong ETag:

```http
Range: bytes=START-END
If-Range: "strong-etag"
Accept-Encoding: identity
```

For a usable modification date, use the date as `If-Range`.

Accept a chunk only when:

- Status is `206`
- `Content-Range` exactly matches requested start and end
- Reported total matches the probe
- Validator remains compatible
- Exactly the requested number of bytes is received

### 8.4 Sequential fallback

Fallback is allowed before destination bytes have been irreversibly emitted when:

- Probe range is ignored
- Probe range metadata is invalid
- Size is unknown
- No usable validator exists
- The origin or proxy does not safely support ranges

For initial probe failure, restart from byte zero as one normal sequential request.

If a later ranged response indicates that the representation changed, stop the current job. Do not merge versions and do not attempt transparent fallback after ordered stdout bytes have already been emitted.

### 8.5 Redirects

- Resolve redirects during probe.
- Apply standard protection against forwarding credentials to a different origin.
- Workers request the resolved final URL directly.
- If later requests redirect to a different object identity, stop and re-probe rather than following silently within an active ranged job.

## 9. FTP and FTPS adapter

### 9.1 Supported mode

- Direct file URLs only
- Passive mode
- Anonymous FTP by default
- Standard user/password credentials when explicitly supplied
- No directory traversal, wildcard expansion, or recursive listing

### 9.2 Metadata

Prefer:

1. `SIZE` for exact length
2. `MDTM` for remote modification time
3. `REST` support for restart/range capability

If exact size or restart capability is unavailable, use one sequential transfer.

### 9.3 Parallel chunks

Each worker owns a control connection and opens data transfers for assigned chunks.

For a chunk:

1. Issue `REST START`.
2. Issue `RETR`.
3. Read exactly `LENGTH` bytes.
4. Close the data transfer after the exact chunk length.
5. Reuse or recreate the control connection depending on server behavior.

Some servers report an aborted transfer after the client intentionally closes at the chunk boundary. Treat this as successful only when the exact requested bytes were received and object metadata remains compatible.

### 9.4 FTPS

- `ftps://` uses explicit TLS by default.
- `--ftps-implicit` selects implicit TLS.
- Verify server certificates using the operating-system trust store.
- Encrypt control and data connections.
- Do not support TLS client certificates.

## 10. Chunk scheduling

### 10.1 No batches

Scheduling is continuous.

With five workers:

```text
start chunks: 0 1 2 3 4
chunk 2 completes -> assign 5
chunk 1 completes -> assign 6
chunk 4 completes -> assign 7
```

The scheduler never waits for a complete group before assigning later chunks.

### 10.2 Work priority

For ordered stream mode:

1. Start an eligible speculative duplicate for the blocking frontier chunk.
2. Retry a required failed chunk whose backoff expired.
3. Assign the next undispatched chunk.
4. Wait for memory, a worker, or retry timing.

For file mode, omit speculative frontier duplication because out-of-order completion does not block writes.

### 10.3 Retry scope

Retries belong to chunks, not worker slots.

A transfer that fails after useful progress requeues that chunk according to `--tries` and retry backoff. It does not immediately reduce concurrency.

A worker slot that repeatedly cannot establish a useful transfer is suppressed independently so the scheduler does not retry unavailable extra connections forever.

## 11. Stream memory model

### 11.1 Hard limit

`--buffer-size` is a hard limit for reserved chunk payload bytes in ordered stream mode.

It includes:

- Active chunk buffers
- Completed out-of-order buffers
- The chunk being written
- A speculative duplicate buffer

Before assigning a stream chunk, reserve its entire actual chunk length.

```text
reserved bytes + next chunk length <= buffer size
```

The normal full-chunk capacity is:

```text
floor(buffer size / split size)
```

The smaller final chunk uses only its actual length.

### 11.2 Memory-limited concurrency

For stream mode:

```text
memory connection limit = floor(buffer size / split size)
initial effective target = min(requested connections,
                               remaining chunk count,
                               memory connection limit)
```

If buffer size is smaller than split size, reject the configuration before downloading.

Example:

```text
connections = 8
split size = 256 MiB
buffer size = 1 GiB
```

Only four full chunk buffers fit, so stream concurrency is limited to four. The process must not reserve 2 GiB or 4 GiB merely because eight connections were requested.

### 11.3 Default window

With defaults:

```text
8 connections * 8 MiB * 2 = 128 MiB
```

Sixteen chunk slots provide roughly one active window and one reorder/look-ahead window.

### 11.4 Backpressure

When no buffer reservation is available:

- Do not assign another chunk.
- Existing workers finish or block according to normal network flow.
- The writer releases reservations as ordered chunks are emitted.

No disk spill is allowed.

## 12. Connection adaptation

### 12.1 Concurrency values

Maintain:

```text
requested maximum
mode-specific maximum
current effective target
currently active count
```

Requested concurrency is never interpreted as a requirement.

### 12.2 Initial admission

Attempt to establish up to the initial effective target.

A worker slot is considered an admission failure when it repeatedly fails before transferring useful response bytes while established workers continue normally. Examples include immediate resets, connection refusal for additional slots, or repeated early protocol failure.

After two consecutive admission failures for one slot:

- Suppress that slot for the current cooldown period.
- Reduce the effective target by one.
- Requeue its chunk for an established or future worker.
- Do not continuously recreate the failed slot.

A successful useful transfer clears that slot's admission-failure count.

### 12.3 HTTP 429 and 503

Treat near-simultaneous 429 or 503 responses as one pressure event.

On a pressure event:

```text
new effective target = max(1, ceil(current target / 2))
```

Do not cancel healthy in-flight transfers. Stop replacing excess workers as they finish.

Honor `Retry-After` when present. Otherwise start with a 30-second recovery cooldown.

429 and 503 are retryable for the affected chunk even if the user did not explicitly list them in `--retry-on-http-error`.

### 12.4 Gradual restoration

The requested maximum remains the recovery target.

After cooldown, restore one slot only when:

- No new pressure event occurred during cooldown.
- Existing workers are making useful progress.
- At least four chunks completed successfully since the last reduction or failed restoration attempt.
- Mode-specific memory capacity permits another worker.

Increase effective target by exactly one.

Do not attempt another increase until:

```text
max(30 seconds, 4 * rolling median successful chunk duration)
```

If the restored slot immediately fails admission or triggers another 429/503:

- Return to the previous target.
- Double the restoration delay.
- Cap restoration delay at five minutes.

A successful restored slot may reduce the delay gradually toward 30 seconds. Do not reset immediately after one success.

This recovery mechanism applies to both explicit 429/503 reductions and suppressed connection slots.

## 13. Speculative duplicate for delayed stream chunks

### 13.1 Scope

Speculative duplication exists only in ordered stream mode because only that mode has a frontier chunk that can block all output.

At most one speculative duplicate may exist globally in the process.

Only the current frontier chunk is eligible.

### 13.2 Eligibility

A duplicate may start when all conditions hold:

1. The original frontier request is still active.
2. No duplicate is already active.
3. The current chunk attempt has not already been hedged.
4. At least four recent non-hedged successful chunk durations exist for comparison.
5. At least two later chunks have completed since the frontier attempt started.
6. Frontier elapsed time is at least twice the rolling median successful chunk duration.
7. Frontier throughput is below half the rolling median worker throughput, or it has made no progress for at least one median chunk duration.
8. One full additional buffer reservation is available.
9. A worker slot is available within the current effective concurrency target.

The duplicate never creates a connection above the effective target. If all allowed workers are occupied, wait for one to become free.

### 13.3 Execution

- Request exactly the same byte range and validator.
- The first complete valid response wins.
- Cancel and discard the slower copy.
- Release the losing buffer reservation.
- Late completion from the canceled copy must not be written.
- A duplicate is not eligible for another duplicate.

The original and duplicate share one logical chunk attempt. One failed copy does not consume a separate full retry budget while the other remains active.

If a duplicate receives 429 or 503, it still triggers normal server-pressure handling.

## 14. Ordered stream sink algorithm

Maintain:

```text
next write index
active chunk map
completed chunk map
reserved payload bytes
fatal error channel
```

Flow:

1. Scheduler reserves memory and starts a chunk.
2. Worker fills its private chunk buffer.
3. Worker validates exact length and protocol metadata.
4. Worker submits the completed buffer by chunk index.
5. Writer checks `next write index`.
6. While the next index is present, write it completely and release its reservation.
7. Advance the frontier.
8. On write error, cancel the job context and discard all buffers.

The writer is the only component that writes to stdout or another sequential destination.

## 15. Positional file sink and sidecar durability

### 15.1 File creation

Create the sidecar before starting out-of-order writes. Ensure the destination is sized as required for the current known object or combined-output segment.

Use positional writes at:

```text
output base offset + chunk start
```

### 15.2 Chunk completion

A file chunk becomes complete in memory only after:

- Exact expected bytes were received
- Positional writes succeeded
- Protocol and validator checks succeeded

### 15.3 Durable checkpoints

Do not persist a completed bitmap bit before corresponding file data is durable.

Checkpoint sequence:

1. Stop taking the sidecar snapshot under a short state lock.
2. Synchronize destination data using `Sync` or the closest portable equivalent.
3. Serialize the current durable completion bitmap.
4. Write `destination.pget.tmp` in the same directory.
5. Synchronize the temporary sidecar.
6. Atomically rename it to `destination.pget`.
7. Synchronize the containing directory where supported.

Chunks completed after the snapshot may be downloaded again after a crash, which is safe. The sidecar must never claim completion for data that was not made durable first.

Checkpoint periodically and on orderly shutdown. A practical v1 policy is when either approximately one second or 64 MiB of newly completed data has accumulated, plus finalization. The exact timer may be tuned without changing the file format.

### 15.4 Finalization

After every chunk is complete:

1. Synchronize destination data.
2. Set exact final length.
3. Apply remote modification time when available.
4. Synchronize metadata where practical.
5. Remove the sidecar.
6. Synchronize the containing directory where supported.

The sidecar is the incomplete-download marker. Its absence after finalization means the destination is complete.

## 16. Resume flows

### 16.1 Single file with sidecar

1. Parse and validate sidecar version.
2. Hash the supplied URL identity and compare it with the recorded job.
3. Probe current remote metadata.
4. Compare size, strong validator, or accepted modification-time identity.
5. Validate destination length and sidecar bitmap dimensions.
6. Schedule only missing chunks.

Any incompatible object identity stops resume before writes begin.

### 16.2 Single file without sidecar

For `-c` only, treat current file length as a contiguous prefix, matching Wget behavior.

This path is intended for files created by another sequential downloader or a previous sequential transfer. It cannot recover arbitrary out-of-order state after a `.pget` sidecar was deleted.

### 16.3 Multiple URLs into one `-O FILE`

Resume requires a `.pget` sidecar.

The sidecar's ordered URL-list hash must exactly match the supplied URLs. Completed earlier items remain complete; the current item resumes from its bitmap; later items remain pending.

Refuse no-sidecar concatenated resume because the current file length alone cannot safely identify which remote object and range produced every existing byte.

### 16.4 Stdout

No resume is available for `-O -` because downstream process state cannot be reconstructed.

## 17. Timestamping flow

For each normal file URL:

1. Probe remote modification time and size.
2. If `-N` and destination exists, compare remote time and size.
3. Skip when the remote object is not newer and sizes match.
4. Otherwise perform the download.
5. Apply remote modification time after successful finalization.

For HTTP, use `Last-Modified` and exact content length when available.

For FTP, use `MDTM` and `SIZE` when available.

With `-O`, disable timestamp comparison after emitting the documented warning.

## 18. Multiple URL flow

### 18.1 Separate destinations

For each URL:

1. Resolve its own filename.
2. Apply collision, no-clobber, continue, and timestamp policy.
3. Run its job to completion or terminal failure.
4. Record exit category.
5. Continue with the next URL.

### 18.2 Concatenated destination

For `-O FILE` or `-O -`:

1. Keep one destination open.
2. Process each URL sequentially.
3. Advance output base offset only after the current URL completes.
4. Stop immediately on the first failure.
5. Never start later URLs after a failed part.

For a seekable `-O FILE`, chunks of the current URL may use positional writes within that URL's assigned output segment. A later URL is not probed or written until the current URL is complete.

## 19. Background process architecture

On supported Unix-like systems:

1. Foreground process parses and validates arguments.
2. If not already marked as the background child, spawn the same executable and arguments with an internal environment marker.
3. Configure child process attributes to create a new session.
4. Redirect stdin to `/dev/null`.
5. Redirect logs to `wget-log`, `-o`, or `-a` target.
6. Do not inherit binary stdout for `-O -`; reject that combination before spawning.
7. Parent prints child PID and exits.
8. Child runs the normal execution plan.

Do not implement a double-fork unless testing shows it is necessary for the supported platforms. A new session plus explicit file descriptor handling is sufficient for the required user model.

## 20. Cancellation and shutdown

Use one root context per active URL.

Cancel it on:

- Fatal protocol inconsistency
- Representation change
- Output write failure
- Exhausted required chunk retries
- User interrupt
- Process shutdown

Workers must:

- Close response bodies and FTP data connections
- Return buffer reservations
- Avoid publishing data after cancellation
- Preserve the last durable sidecar checkpoint in file mode

On SIGINT or SIGTERM:

- Stop assigning new chunks.
- Cancel active network operations.
- Checkpoint file-mode state if it can be done safely and promptly.
- Exit non-zero.

## 21. Retry and timeout policy

### 21.1 Retryable events

- Connection reset
- Temporary DNS or dial failure
- Read timeout with incomplete chunk
- Short body
- HTTP 429
- HTTP 503
- User-selected HTTP status codes
- FTP temporary failure classes

### 21.2 Non-retryable events

- Invalid `Content-Range`
- Changed object validator
- Destination write failure
- TLS certificate verification failure unless explicitly disabled
- Authentication failure without changed credentials
- Unsupported protocol capability after sequential fallback is impossible

### 21.3 Backoff

Use bounded exponential backoff with jitter for chunk retries. Respect `Retry-After` for 429 and 503.

Retry delays must not block unrelated established workers for the same active URL. Only the affected chunk waits.

## 22. Security and privacy

### 22.1 TLS

- Verify server certificates by default.
- Use the operating-system trust store.
- `--no-check-certificate` disables verification only when explicitly requested.
- No client-certificate or private-key loading.

### 22.2 Credentials

- Never log URL userinfo, passwords, or authorization header values.
- Remove userinfo from display URLs.
- Do not persist credentials in sidecars.
- Do not forward credentials across unrelated redirect origins.

### 22.3 Output paths

Sanitize URL and `Content-Disposition` filenames:

- Remove directory separators
- Reject NUL bytes
- Reject `.` and `..`
- Prevent absolute paths
- Keep all derived names in the selected destination directory

### 22.4 Sidecar integrity

The sidecar is local state, not trusted remote input. Validate all lengths, indexes, bitset sizes, paths, and version fields before use.

## 23. Scalability and resource limits

### Network

The main scaling unit is one active URL with up to the requested connection count. There is no multiplication by simultaneous URL jobs.

### Memory

Stream payload memory is bounded by `--buffer-size`.

File mode uses per-worker I/O buffers rather than full-chunk buffers, plus sidecar bitmap memory.

### Disk

File mode uses the destination and small sidecar only. Stream mode uses no temporary disk storage.

### Goroutines

Use a bounded set:

- One job coordinator
- Up to effective worker count
- One stream writer or file checkpoint coordinator
- One progress reporter
- Small cancellation and signal helpers

Do not create one goroutine per planned chunk.

## 24. Reliability invariants

The implementation must preserve these invariants:

1. URLs are processed in supplied order.
2. Stream bytes are emitted in exact source order.
3. Stream reserved chunk payload memory never exceeds `--buffer-size`.
4. At most one speculative duplicate exists.
5. A speculative duplicate never exceeds effective connection concurrency.
6. A file sidecar never marks data durable before the destination data is synchronized.
7. Ranges from incompatible object versions are never combined.
8. Later concatenated URLs are never written after an earlier URL fails.
9. Requested concurrency is a maximum, not a permanent retry obligation.
10. Chunk size never changes during an object download.

## 25. Testing strategy

### 25.1 Unit tests

- CLI option parsing and conflicts
- Filename and collision rules
- Chunk boundary calculations
- Stream reservation accounting
- Continuous scheduling without batch barriers
- Completion bitmap encoding and validation
- Sidecar atomic update logic
- Validator comparison
- Concurrency reduction and restoration state machine
- Speculative duplicate eligibility and first-winner behavior
- Exit-status aggregation

### 25.2 HTTP integration server

Build a deterministic test origin that can simulate:

- Valid ranges
- Ignored ranges
- Invalid `Content-Range`
- No validator
- Strong and weak ETags
- Object changes during download
- Per-connection delays
- One abnormally slow chunk
- Connection refusal above a configured count
- 429 and 503 with and without `Retry-After`
- Short bodies and resets
- Redirects
- High response latency
- Authentication and credential redaction

### 25.3 FTP integration server

Test:

- `SIZE`, `MDTM`, and `REST`
- Missing restart support
- Parallel data connections
- Servers that return transfer-aborted status after exact partial read
- Explicit and implicit FTPS
- Certificate verification failure

### 25.4 End-to-end pipeline tests

Compare hashes for:

```bash
pget -qO- URL | sha256sum
pget -qO- part1 part2 part3 | sha256sum
```

Test with slow consumers, early consumer exit, decompression, and forced process interruption.

### 25.5 Crash recovery tests

Terminate during:

- Destination write
- Before destination sync
- During sidecar temporary write
- After sidecar sync but before rename
- After final data sync but before sidecar removal

Resume must redownload uncertain chunks but never trust undurable data.

## 26. Rejected alternatives

### Wrap aria2

Rejected because aria2 is file-oriented, does not provide a clean ordered stdout stream, and would reintroduce temporary storage or fragile file-tailing synchronization.

### Python implementation

Rejected because Go provides a simpler executable deployment model and cleaner bounded concurrency, cancellation, positional I/O, and process detachment for this tool.

### Rust implementation

Technically suitable, but explicitly not selected. Go is sufficient and simpler for the intended implementation team.

### Batch chunk scheduling

Rejected because one slow chunk would idle every other connection at each batch boundary.

### Temporary chunk files for stdout

Rejected because they consume disk, duplicate I/O, require cleanup, and violate the primary no-full-download streaming use case.

### HTTP/2 multiplexing

Rejected for parallel mode because multiple ranges over one TCP connection do not address per-TCP-flow limitations. Use separate persistent HTTP/1.1 connections.

### Automatic chunk resizing

Rejected for v1 because it makes memory accounting, reproducibility, retries, sidecar state, and performance analysis less predictable.

### Concurrent separate URLs

Rejected because it conflicts with Wget ordering and multipart concatenation semantics.

## 27. Risks and mitigations

### Head-of-line blocking in stream mode

A slow frontier chunk blocks output even when later chunks are ready.

Mitigation: continuous look-ahead, doubled default buffer window, progress timeout, retry, and one speculative duplicate.

### Origins that dislike partial FTP transfers

Some FTP servers may treat exact-length client close as abnormal.

Mitigation: validate exact bytes, handle expected temporary status carefully, reconnect control sessions when needed, and fall back to sequential FTP.

### Excessive user-selected split size

A large split size can make stream concurrency impossible under the memory limit.

Mitigation: compute memory-limited concurrency and reject only when one chunk cannot fit at all.

### False concurrency recovery

A restored slot may recreate server pressure.

Mitigation: additive increase, long recovery interval, and exponential restoration backoff.

### Sidecar deletion

Deleting a sidecar loses arbitrary-piece knowledge.

Mitigation: clearly document that no-sidecar `-c` assumes a contiguous prefix and require a sidecar for concatenated resume.

### Remote object mutation

Parallel requests can otherwise combine different versions.

Mitigation: require usable validators, send `If-Range`, validate every response, and stop on inconsistency.

## 28. Deployment assumptions

- Primary target: Linux and Unix-like systems
- One Go executable
- No configuration service
- No runtime daemon
- No mandatory external helper executable
- Pure-Go dependencies preferred so CGO can remain disabled
- Logs to stderr, selected files, or `wget-log` in background mode

## 29. Final architecture boundary

`pget` is a single-process, sequential-URL downloader. For the active object, it uses fixed-size ranges over a bounded adaptive connection pool. Seekable destinations receive positional writes with a durable `.pget` bitmap sidecar. Non-seekable destinations receive an ordered, hard-memory-limited stream with one optional speculative duplicate for a delayed frontier chunk.

There are no unresolved technical architecture questions for v1.
