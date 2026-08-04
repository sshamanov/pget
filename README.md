# pget

Wget-style command-line downloader with parallel chunked retrieval and ordered streaming.

`pget` downloads HTTP, HTTPS, FTP, and FTPS objects. Its defining capability is parallel retrieval of byte ranges from one remote file while preserving a single ordered output stream — allowing large files, compressed archives, and multipart objects to be piped directly into another process without first storing them on disk.

```bash
pget https://example.org/image.iso
pget -O image.iso https://example.org/image.iso
pget -qO- https://example.org/archive.tar.zst | zstd -d | tar -x
pget -O - part-001 part-002 part-003 | tar -x
```

The command surface and default behavior follow GNU Wget where practical.

## Installation

```bash
go install github.com/sshamanov/pget@latest
```

Prebuilt static Linux AMD64 binaries are published on every push to `main`.

## Document authority

| File | Scope |
|------|-------|
| `README.md` (this file) | Overview, key architecture, deployment |
| `DESIGN.md` | Product and CLI design (command surface, user-visible behavior) |
| `ARCHITECTURE.md` | Technical architecture (components, algorithms, data model) |
| `AGENTS.md` | Development, testing, CI, and agent instructions |

## Key architecture decisions

1. **Sequential URLs** — URLs are processed strictly in order. Only one URL is active at a time; internal chunk concurrency applies only to the active URL. Cross-URL parallelism is explicitly rejected.

2. **Fixed chunk size** — Chunks are fixed for the lifetime of one object (default 8 MiB). No automatic resizing. Concurrency adapts via connection count adjustments, not chunk size changes.

3. **Two output sinks** — Stream sink (ordered, bounded memory, for `-O -` and non-seekable outputs) and file sink (positional writes with `.pget` sidecar for resumable downloads).

4. **Continuous scheduling** — Chunks are assigned continuously (no batches). When a worker finishes, the next undispatched chunk is assigned immediately.

5. **Connection adaptation** — Concurrency is a maximum, not a requirement. Workers are suppressed on repeated admission failures. HTTP 429/503 halves the effective target. Recovery is additive with backoff.

6. **Speculative duplicate** — In stream mode, at most one duplicate of the blocking frontier chunk may be started when specific eligibility conditions are met. First valid response wins.

7. **Sidecar durability** — File data is synchronized before the sidecar bitmap is updated. The sidecar must never claim completion for data that wasn't made durable first.

8. **No temporary disk for stream mode** — `-O -` uses bounded memory only, hard-limited by `--buffer-size` (default 128 MiB).

## Component architecture

```
CLI parser → Execution planner → Job runner (per-URL loop)
                                    ├── Protocol adapter (HTTP/HTTPS, FTP/FTPS)
                                    ├── Chunk planner
                                    ├── Scheduler + Connection controller
                                    ├── Ordered stream sink (stdout / non-seekable)
                                    └── Positional file sink + Sidecar manager
```

- **CLI parser** — Wget-style option parsing (options interspersed with URLs, combined short flags, `--name=value`).
- **Protocol adapters** — Common interface (`Probe`, `OpenRange`, `OpenSequential`) with protocol-specific validation for HTTP/HTTPS and FTP/FTPS.
- **Scheduler** — Continuous chunk assignment, retries, memory reservations, speculative duplicates.
- **Sidecar manager** — Versioned JSON with base64-encoded completion bitmap for resumable downloads.

## Deployment

- Single statically-linked Go executable (CGO disabled for maximum Linux compatibility, including Ubuntu 14.04)
- Primary target: Linux and Unix-like systems
- No configuration service, daemon, or external helper executables
- Wget-compatible option names and semantics where implemented
