# rust_weather_api — In-Memory NetCDF Merge Server

A Rocket HTTP server that accepts two NetCDF files, merges them entirely in
memory (no disk I/O at any point), and returns the merged file.

---

## Build & Run

### Prerequisites

The [netCDF-C library](https://github.com/Unidata/netcdf-c) must be installed.

```bash
# macOS
brew install netcdf

# Ubuntu / Debian
sudo apt-get install libnetcdf-dev

# Fedora / RHEL
sudo dnf install netcdf-devel
```

### Run

```bash
cargo run
```

The server starts on `http://127.0.0.1:8000` by default (Rocket's own default).
Set `ROCKET_ADDRESS=0.0.0.0` to listen on all interfaces.

---

## Docker

No local Rust toolchain or netCDF libraries needed — everything is built and
run inside the container.

### Build

```bash
docker build -t netcdf-merge-server .
```

### Run

```bash
docker run --rm -p 8000:8000 netcdf-merge-server
```

The server is now reachable at `http://localhost:8000`. Use the same `curl`
commands shown below; they work identically against the container.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `ROCKET_ADDRESS` | `0.0.0.0` | Bind address inside the container |
| `ROCKET_PORT` | `8000` | Listening port |

Override at runtime if needed:

```bash
docker run --rm -p 9000:9000 -e ROCKET_PORT=9000 netcdf-merge-server
```

---

## Testing

### Unit & integration tests

Requires the netCDF-C development headers on the host machine.

```bash
# macOS
brew install netcdf

# Ubuntu / Debian
sudo apt-get install libnetcdf-dev
```

```bash
# Run all tests (single-threaded — required because HDF5 uses global C state)
cargo test -- --test-threads=1
```

This runs:
- **7 unit tests** in `src/netcdf_merge.rs` — merge logic, dimension conflicts, attribute conflicts, invalid input
- **11 integration tests** in `tests/api_test.rs` — all HTTP routes, status codes, name isolation, overwrite behaviour

### End-to-end tests (Docker)

```bash
# Build the image, start the container, run all E2E tests, then clean up
bash scripts/e2e_test.sh
```

Or manually:

```bash
docker run --rm -d --name netcdf-test -p 8000:8000 netcdf-merge-server
python3 scripts/e2e_test.py          # 26 scenarios
docker rm -f netcdf-test
```

The E2E suite (`scripts/e2e_test.py`) covers: missing-data 404s, successful uploads and merges, name isolation, invalid-bytes 500, and overwrite behaviour.  It uses Python's standard library only — no third-party packages required.

---

## curl Examples

### Upload part_a

```bash
curl -X POST "http://localhost:8000/part_a?name=mydata" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @/path/to/file_a.nc
```

### Upload part_b

```bash
curl -X POST "http://localhost:8000/part_b?name=mydata" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @/path/to/file_b.nc
```

### Merge and download

```bash
curl "http://localhost:8000/read?name=mydata" -o merged.nc
```

---

## API Reference

| Method | Path | Query | Body | Response |
|--------|------|-------|------|----------|
| POST | `/part_a` | `name=NAME` | raw NetCDF bytes | `200 OK` |
| POST | `/part_b` | `name=NAME` | raw NetCDF bytes | `200 OK` |
| GET  | `/read`   | `name=NAME` | — | `200 OK` + merged NetCDF bytes, or error |

### Error codes

| Code | Meaning |
|------|---------|
| 404  | `part_a` or `part_b` not yet uploaded for the given `name` |
| 400  | Dimension conflict (same name, different length/unlimited-ness) or unsupported variable type (strings, user-defined types) |
| 413  | Upload body exceeds 256 MiB limit |
| 500  | Unexpected internal error |

---

## Merge Logic

1. **Dimensions** — union of all dimensions from both files.  If the same
   dimension name appears in both but with a different length *or* a
   different unlimited-ness, the request is rejected with `400`.
2. **Global attributes** — part_a wins on conflicts (part_b attribute is
   silently dropped if the name already exists).
3. **Variables** — part_a wins on conflicts; variables unique to part_b are
   appended.

### Known Limitation — Strings and User-Defined Types

`nc_copy_var` may return an error for netCDF-4 string variables or
user-defined types (enums, vlens, compounds).  In that case the server
returns `400` with a message identifying the offending variable.  Classic
numeric variables (`NC_BYTE` … `NC_DOUBLE`) are always supported.

---

## No-Disk-I/O Guarantee

| API used | Purpose |
|----------|---------|
| `nc_open_mem` | Open part_a / part_b from a `*mut c_void` buffer |
| `nc_create_mem` | Create the output file in a growable heap buffer |
| `nc_close_memio` | Flush and extract the output bytes; returns a `NC_memio` struct whose `.memory` pointer is freed with `libc::free` after copying |

No `std::fs`, no `tempfile`, no file paths are involved at any point.

---

## Parallel Request Handling

### Store-Level Races

A single `RwLock<HashMap<String, Entry>>` guards the in-memory store.
Because POST /part_a and POST /part_b are separate requests, a GET /read
arriving between them will see one part uploaded and one still absent —
returning 404 — which is correct and safe.  A more subtle race would be:

```
Thread 1: POST part_a (name=x) — write lock, insert
Thread 2: POST part_b (name=x) — write lock, insert
Thread 3: GET  read   (name=x) — read lock, snapshots Arc<part_a> & Arc<part_b>
```

The **snapshot approach** in `/read` mitigates this: the handler acquires a
read lock only long enough to clone two `Arc` pointers, then immediately
drops the lock.  The merge runs on the cloned buffers without holding any
lock, so writers are never blocked during the (potentially slow) netCDF
work.  The worst that can happen is that a read sees the state between two
uploads, yielding a 404 — not corruption.

### netCDF-C / HDF5 Thread-Safety Issues

The netCDF-C library (and the HDF5 library it links against in netCDF-4
mode) use **global mutable state** (file-ID tables, type registries, memory
allocators).  They are not re-entrant.  Calling any `nc_*` function from two
threads simultaneously — even on different file IDs — can cause heap
corruption, crashes, or silent data corruption.

The `netcdf-sys 0.3` crate does not expose a serialisation lock, so this
project defines its own `pub static NC_LOCK: std::sync::Mutex<()>` in
`src/netcdf_merge.rs`.  Every `nc_*` call — in both the server merge path
and the test helpers — acquires this lock first.

### Mitigations (implemented and recommended)

| Mitigation | Status | Notes |
|-----------|--------|-------|
| `NC_LOCK` global mutex | ✅ implemented | Defined in `src/netcdf_merge.rs`; serialises all `nc_*` calls |
| `spawn_blocking` for merge | ✅ implemented | Keeps Tokio workers free while C code runs |
| Arc snapshot before releasing RwLock | ✅ implemented | Prevents holding store lock during merge |
| Per-key lock | 🔲 not implemented | Would allow concurrent merges for different keys; safe only if each merge still acquires `NC_LOCK` anyway — no throughput gain until netCDF-C itself is thread-safe |
| Upload size limits (256 MiB) | ✅ implemented | Protects against memory exhaustion |
| Worker-process isolation / queue | 🔲 best option for production | Spawn one worker process that owns all netCDF I/O; communicate over channels or a local socket.  The worker processes requests sequentially, eliminating all C-library concurrency concerns while the main server remains fully async. |

**Recommended production architecture**: run a dedicated `netcdf-worker`
process (single-threaded) that owns all `nc_*` calls, and have the Rocket
server forward merge work to it via an async channel or gRPC.  This
completely decouples HTTP concurrency from C-library serialisation
constraints.
