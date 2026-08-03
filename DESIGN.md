# pget Product and CLI Design

Status: Final

## 1. Product summary

`pget` is a Wget-style command-line downloader for HTTP, HTTPS, FTP, and FTPS.

Its defining capability is parallel retrieval of ranges from one remote file while preserving a single ordered output stream. This allows large remote files, compressed archives, and multipart objects to be piped directly into another process without first storing the complete input.

Examples:

```bash
pget https://example.org/image.iso
pget -O image.iso https://example.org/image.iso
pget -qO- https://example.org/archive.tar.zst | zstd -d | tar -x
pget -O - part-001 part-002 part-003 | tar -x
```

The command surface and default behavior follow GNU Wget where practical. Parallel download controls are added only where Wget has no equivalent behavior.

## 2. Users

### Interactive command-line user

Downloads one or more files, expects familiar Wget option names, readable progress, safe filename behavior, and useful retry handling.

### Pipeline user

Streams a large remote object or an ordered series of object parts into a decompressor, extractor, hasher, image writer, or another command.

### Automation operator

Runs non-interactive or background downloads, resumes interrupted transfers, uses timestamp checks, and relies on stable exit statuses and logs.

## 3. Product goals

- Preserve familiar Wget command naming and file behavior.
- Use several connections for one remote object when safe and useful.
- Emit parallel downloads in exact byte order when writing to stdout.
- Process multiple URLs strictly in the order supplied.
- Support resumable file downloads through a `.pget` sidecar.
- Bound stream buffering with a hard memory limit.
- Fall back to a normal sequential transfer when safe parallel retrieval is unavailable.
- Support timestamp synchronization.
- Support Unix-style background execution.
- Keep deployment to one Go executable.

## 4. Non-goals

The first version does not provide:

- Recursive downloading
- Website mirroring
- HTML link extraction or conversion
- Page requisites
- Cookie persistence
- `robots.txt` processing
- Parallel downloading of separate URLs
- A persistent daemon, RPC service, or download queue
- BitTorrent, Metalink, or mirror selection
- Automatic chunk resizing
- Automatic performance presets
- TLS client-certificate authentication
- FTP directory traversal, wildcard expansion, or recursive FTP retrieval

## 5. Interface principles

### Wget behavior wins

When Wget already defines an option name or user-visible behavior, `pget` follows Wget unless the behavior is incompatible with ordered parallel streaming.

### One URL sequence, one order

Separate URLs are never downloaded concurrently. The active URL may use several connections internally, but URL ordering is never changed.

### No hidden disk spill for stdout

`-O -` uses bounded memory. It does not silently write downloaded chunks to temporary files.

### Safe fallback

Failure to establish safe parallel retrieval is not normally fatal. `pget` falls back to one sequential connection before producing output.

### Stable, scriptable output

Downloaded bytes go only to the selected destination. Progress and diagnostics go to stderr or the selected log file.

## 6. Command model

```text
pget [OPTION]... [URL]...
```

The parser shall support:

- Options before or after URLs
- `--` to end option parsing
- Long options using `--name=value` or `--name value`
- Short options using Wget-compatible spelling
- Combined short flags when none of the combined options consumes an argument

## 7. Required command-line surface

### Startup

```text
-V, --version
-h, --help
```

### Logging and status

```text
-q,  --quiet
-v,  --verbose
-nv, --no-verbose
-S,  --server-response
-o,  --output-file=FILE
-a,  --append-output=FILE
```

`-o` replaces the log file. `-a` appends to it.

### Download behavior

```text
-O,  --output-document=FILE
-c,  --continue
-nc, --no-clobber
-N,  --timestamping
-b,  --background
-i,  --input-file=FILE
-t,  --tries=NUMBER
-T,  --timeout=SECONDS
     --connect-timeout=SECONDS
     --read-timeout=SECONDS
     --retry-connrefused
     --retry-on-http-error=CODES
     --content-disposition
     --no-use-server-timestamps
     --spider
     --progress=TYPE
```

### Request metadata

```text
     --header=STRING
     --user-agent=STRING
     --referer=URL
```

### TLS and FTPS

```text
     --no-check-certificate
     --ftps-implicit
```

TLS is used for encrypted transport and normal server-certificate verification. TLS client certificates and mutual TLS are out of scope.

### Parallel retrieval

```text
     --connections=NUMBER
     --split-size=SIZE
     --buffer-size=SIZE
     --no-parallel
```

Parallel options use long names only to avoid conflicts with Wget short options.

## 8. Defaults

```text
connections: 8
split size:  8 MiB
buffer size: 128 MiB
```

`--buffer-size` is a hard limit for chunk payload memory in ordered stream mode. It is not a guarantee about total process RSS because runtime, TLS, HTTP, and bookkeeping allocations are additional.

There is no automatic chunk-size adjustment.

## 9. Filename behavior

### URL-derived output

```bash
pget https://example.org/files/archive.tar.zst
```

creates:

```text
archive.tar.zst
```

By default, the filename comes from the final redirected URL path.

`Content-Disposition` affects filename selection only when `--content-disposition` is specified.

If no usable name exists, use `index.html`.

### Existing file

Without `-c`, `-nc`, or `-O`, preserve the existing file and select a numbered name:

```text
archive.tar.zst
archive.tar.zst.1
archive.tar.zst.2
```

With `-nc`, skip that URL when its destination already exists.

### Explicit output

```bash
pget -O destination.bin URL
```

writes to exactly `destination.bin`, following Wget semantics.

```bash
pget -O - URL
```

writes downloaded bytes to stdout.

## 10. Multiple URLs

### Normal file mode

```bash
pget URL1 URL2 URL3
```

Behavior:

1. Download URL1, using parallel ranges internally when possible.
2. Finish or exhaust retries for URL1.
3. Download URL2.
4. Download URL3.

If one URL fails, continue with later URLs. The final exit status is non-zero if any URL failed.

### Single-output mode

```bash
pget -O combined.bin URL1 URL2 URL3
pget -O - URL1 URL2 URL3 | consumer
```

The output is the exact concatenation:

```text
bytes(URL1) + bytes(URL2) + bytes(URL3)
```

URLs remain sequential. Bytes from different URLs are never interleaved.

If any URL fails in single-output mode, stop immediately. Do not append later URLs after a missing part.

## 11. Ordered stream mode

Ordered stream mode is active for `-O -` and for any other output path that cannot accept positional writes.

The user-visible guarantees are:

- Output bytes are identical to a sequential download.
- Chunks may arrive out of order internally.
- Chunks are emitted only when every preceding byte is available.
- Memory use is bounded by `--buffer-size` for chunk payloads.
- When the memory limit is reached, new chunks are not scheduled.
- No temporary chunk files are created.
- Stream mode cannot resume after process restart.

Example:

```bash
pget -qO- archive.tar.zst | zstd -d | tar -x
```

All progress and errors remain off stdout.

## 12. File mode and sidecar behavior

Normal file output uses the destination file plus a sidecar:

```text
image.iso
image.iso.pget
```

The destination may contain completed ranges written out of order. The `.pget` file marks it as incomplete and records enough state to resume safely.

On successful completion:

1. Flush and synchronize the destination.
2. Apply the remote modification time when available.
3. Remove the `.pget` sidecar.

A sidecar is not created for `-O -`.

## 13. Continue behavior

```bash
pget -c URL
pget -c -O image.iso URL
```

When a matching `.pget` sidecar exists, resume only missing chunks.

For a single URL, when no sidecar exists, `-c` follows Wget-style contiguous resume from the current file length. This assumes the existing file is a valid contiguous prefix. Deleting a `.pget` sidecar from an incomplete out-of-order `pget` download makes safe resume impossible.

Resume is refused when the remote object clearly differs from the sidecar state, including incompatible size or representation validators.

For multiple URLs combined with `-O FILE`, resume requires a sidecar. The sidecar stores the complete ordered URL list, and the supplied URLs must match in the same order. File length alone is not sufficient to infer safe multipart resume state.

## 14. Timestamp synchronization

### Normal completion

When the remote modification time is available, set the completed local file modification time to that value unless `--no-use-server-timestamps` is explicitly selected.

### Timestamp checking

```bash
pget -N URL
```

Skip downloading when:

- The destination exists.
- The remote file is not newer.
- The reported sizes match.

Download when the local file is missing, the remote file is newer, or sizes differ.

For HTTP and HTTPS, use `Last-Modified` and content length when available.

For FTP and FTPS, prefer direct metadata commands such as `MDTM` and `SIZE`.

`-N` is incompatible with meaningful destination comparison under `-O`. Emit a warning and disable timestamp comparison for that invocation.

## 15. Background mode

```bash
pget -b URL...
pget -b -o download.log URL...
```

Background mode follows Wget's user model:

- The foreground process validates arguments and starts a detached child.
- The foreground process prints the child PID and exits.
- The child continues the same URL sequence.
- Standard input is detached.
- Logs go to `wget-log` unless `-o` or `-a` selects another file.
- There is no daemon, queue manager, RPC interface, or reattachment mechanism.

Example status:

```text
Continuing in background, pid 18432.
Output will be written to 'wget-log'.
```

Reject `-b -O -` because detached binary stdout has no safe implicit destination.

The v1 background implementation is required on Linux and other supported Unix-like systems. Unsupported platforms must fail clearly rather than pretending to detach.

## 16. Protocol behavior

### HTTP and HTTPS

- Follow redirects using standard safe redirect rules.
- Disable transparent content decoding for ranged retrieval.
- Attempt parallel ranges only when object size and representation consistency can be validated.
- Fall back to one sequential request when ranges are ignored, invalid, or unsafe.

### FTP and FTPS

- Support direct file URLs only.
- Use passive mode.
- Use anonymous FTP when no credentials are supplied.
- Allow credentials through standard URL/user options without adding TLS client authentication.
- Use `SIZE`, `MDTM`, and `REST` when supported.
- Fall back to one sequential transfer when safe parallel restart is unavailable.
- Use explicit FTPS by default for `ftps://`; `--ftps-implicit` selects implicit FTPS.
- Encrypt both control and data connections.

## 17. Progress and diagnostics

### Interactive terminal

Show:

- Source URL
- Connection and response status
- Destination name
- Remote size when known
- Current bytes and percentage
- Aggregate transfer rate
- Estimated completion time when meaningful
- Effective connection count when parallel mode is active
- A clear indication when parallel mode falls back to sequential mode

### Non-interactive stderr

Do not emit terminal control sequences. Use periodic line-oriented updates or completion-only output according to `--progress` and verbosity.

### Quiet modes

- `-q`: no normal progress or informational messages
- `-nv`: start, completion, warnings, and errors without a live progress display
- `-v`: detailed connection, retry, fallback, and sidecar diagnostics
- `-S`: include protocol response information without writing it to stdout

Progress must never depend on color alone.

## 18. Important user-visible states

### Parallel mode accepted

Report the requested and current effective connection counts in verbose output.

### Parallel mode unavailable

Before emitting destination bytes, fall back to a sequential transfer and report the reason unless quiet mode is active.

### Server pressure

On HTTP 429 or 503, reduce active concurrency, honor `Retry-After` when supplied, and later try to restore concurrency gradually.

### Slow blocking chunk

In ordered stream mode, one speculative duplicate may be started for the chunk currently blocking output. This is internal behavior; verbose output may report it.

### Representation changed

Stop the current URL. Never combine ranges from incompatible versions of a remote object.

### Broken output

If the destination or stdout write fails, cancel active network work and return a file I/O error.

### Interrupted process

- File mode retains the destination and `.pget` sidecar for `-c`.
- Stream mode cannot be resumed.

## 19. Exit status

Use Wget-compatible exit categories:

```text
0  Success
1  Generic error
2  Command-line or configuration parse error
3  File I/O error
4  Network failure
5  TLS certificate verification failure
6  Authentication failure
7  Protocol error
8  Server error response
```

For multiple normal file downloads, return the highest-priority non-zero category observed. For `-O` concatenation, stop at the first error and return its category.

## 20. Accessibility and terminal compatibility

- Work correctly without a TTY.
- Never require color.
- Respect quiet and no-verbose modes.
- Avoid rapid full-screen redraws.
- Keep error messages line-oriented and copyable.
- Include the affected URL and destination in actionable error messages.
- Do not expose credentials or authorization headers in logs.

## 21. Final product boundary

`pget` is a Wget-style downloader that processes URLs sequentially, accelerates the active file through parallel fixed-size ranges, writes resumable files with a `.pget` sidecar, and can reorder those ranges into a bounded-memory stdout stream.

There are no unresolved product or CLI decisions for v1.
