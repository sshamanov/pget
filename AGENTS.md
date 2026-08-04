# AGENTS.md

This file provides guidance to Claude Code and other AI coding agents when working in this repository.

## Document authority

| File | Scope | Authority |
|------|-------|-----------|
| `README.md` | Overview, key architecture, deployment | Project overview and installation |
| `DESIGN.md` | Product and CLI design | User-visible behavior, option semantics, exit statuses |
| `ARCHITECTURE.md` | Technical architecture | Components, algorithms, data model, scheduling |
| `AGENTS.md` (this file) | Development, testing, CI | How to build, test, and ship — agents read this first |

## Build and development

All development and testing happens in Docker. Do not rely on system-installed tools. Use `network_mode: host` for the container.

```bash
# Run tests
docker run --rm --network host -v "$(pwd)":/src -w /src \
    golang:1.24-alpine go test ./... -count=1 -v

# Run tests with race detection
docker run --rm --network host -v "$(pwd)":/src -w /src \
    golang:1.24-alpine go test ./... -race -count=1

# Run a single test
docker run --rm --network host -v "$(pwd)":/src -w /src \
    golang:1.24-alpine go test -run TestName ./path/to/package

# Build a binary locally (for ad-hoc testing; official builds happen in CI)
docker run --rm --network host -v "$(pwd)":/src -w /src \
    golang:1.24-alpine sh -c \
    'CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bin/pget .'

# Lint
docker run --rm --network host -v "$(pwd)":/src -w /src \
    golang:1.24-alpine go vet ./...

# Enter a dev shell
docker run --rm --network host -v "$(pwd)":/src -w /src -it \
    golang:1.24-alpine sh
```

## Git discipline

Use git for every change. Commit often with clear, atomic messages. Do not leave uncommitted work that mixes unrelated changes.

Before pushing:
- All tests must pass
- End-to-end tests must pass

## Testing requirements

Cover everything with tests. Run tests at these moments:

- After completing any non-trivial function or component
- Before committing
- Before pushing (always)

### Test categories

**Unit tests** — CLI option parsing and conflicts, filename and collision rules, chunk boundary calculations, stream reservation accounting, continuous scheduling, completion bitmap encoding and validation, sidecar atomic update logic, validator comparison, concurrency reduction and restoration state machine, speculative duplicate eligibility, exit-status aggregation.

**HTTP integration server** — A deterministic test origin simulating valid ranges, ignored ranges, invalid `Content-Range`, no validator, strong and weak ETags, object changes mid-download, per-connection delays, one abnormally slow chunk, connection refusal above a configured count, 429 and 503 with and without `Retry-After`, short bodies and resets, redirects, high response latency, authentication and credential redaction.

**FTP integration server** — Test `SIZE`, `MDTM`, `REST`, missing restart support, parallel data connections, transfer-aborted status after partial read, explicit and implicit FTPS, certificate verification failure.

**End-to-end pipeline tests** — Hash comparison for `pget -qO- URL | sha256sum`, concatenated parts, slow consumers, early consumer exit, decompression, forced process interruption. These must pass before every push.

**Crash recovery tests** — Terminate during destination write, before destination sync, during sidecar temporary write, after sidecar sync but before rename, after final data sync but before sidecar removal. Resume must redownload uncertain chunks but never trust undurable data.

## CI and publishing

GitHub Actions (`.github/workflows/build.yml`) is the **sole build and publish mechanism**. On every push to `main`:

1. Checkout + setup Go 1.24
2. Run tests with race detection
3. Build static Linux AMD64 binary (`CGO_ENABLED=0`, `-ldflags="-s -w"`)
4. Upload binary as workflow artifact
5. Upload binary to the file service for distribution

The binary is not pushed as a Docker image. Distribution is via `go install` and the file service.

### Binary compatibility

Build with `CGO_ENABLED=0` for a fully static binary that runs on old Linux systems including Ubuntu 14.04. No glibc dependency, no dynamic linking.

### go install

The module is `github.com/sshamanov/pget`. Users can install the binary directly:

```bash
go install github.com/sshamanov/pget@latest
```

Keep the module path and import paths consistent with this canonical form.
