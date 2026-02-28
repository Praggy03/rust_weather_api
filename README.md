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
```

```bash
cargo run
```

The server starts on `http://127.0.0.1:8000` by default.

---

## Docker

No local Rust toolchain or netCDF libraries needed.

```bash
docker build -t netcdf-merge-server .
docker run --rm -p 8000:8000 netcdf-merge-server
```

---

## Testing

```bash
# unit + integration (single-threaded required — HDF5 uses global C state)
cargo test -- --test-threads=1

# E2E against a live container
docker run --rm -d --name netcdf-test -p 8000:8000 netcdf-merge-server
python3 scripts/e2e_test.py
docker rm -f netcdf-test
```

---

## curl Examples

```bash
curl -X POST "http://localhost:8000/part_a?name=mydata" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @file_a.nc

curl -X POST "http://localhost:8000/part_b?name=mydata" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @file_b.nc

curl "http://localhost:8000/read?name=mydata" -o merged.nc
```

---

## API

| Method | Path | Query | Response |
|--------|------|-------|----------|
| GET  | `/health` | — | `200 ok` |
| POST | `/part_a` | `name=NAME` | `200 OK` |
| POST | `/part_b` | `name=NAME` | `200 OK` |
| GET  | `/read`   | `name=NAME` | `200 OK` + merged NetCDF bytes |

**Error codes:** `404` if either part hasn't been uploaded yet, `400` on a
dimension conflict or unsupported variable type, `413` if the upload exceeds
256 MiB, `500` for unexpected errors.

---

## Merge Logic

- **Dimensions** — union of both files. Same name with different lengths → `400`.
- **Global attributes** — union of both; part_a wins on conflicts.
- **Variables** — union of both; part_a wins on conflicts.

`nc_copy_var` may fail on NetCDF-4 string variables or user-defined types
(enums, compounds) — the server returns `400` in that case. Standard numeric
types are always supported.

All merge I/O goes through `nc_open_mem` / `nc_create_mem` / `nc_close_memio`
— no file paths, no `std::fs`, no temp files anywhere.

---

## Parallel Request Handling

There are two separate problems here.

**Store races.** The in-memory store is a `RwLock<HashMap>`. Because part_a
and part_b arrive as separate requests, a `/read` between them will 404 —
that's expected. The subtler issue is holding the lock during a slow merge.
The `/read` handler solves this by cloning two `Arc` pointers under a brief
read lock and then dropping it immediately, so the actual merge runs lock-free
and writers are never blocked.

**netCDF-C / HDF5 thread safety.** The netCDF-C library uses global C state
(file ID tables, internal allocators) and is not re-entrant. Calling any
`nc_*` function from two threads simultaneously — even on different file IDs —
can corrupt memory. This project serialises all `nc_*` calls through a single
`static Mutex` (`NC_LOCK` in `netcdf_merge.rs`), which makes merges sequential.
The merge itself runs on a `spawn_blocking` thread so async workers aren't
stalled.

This is correct but not scalable under high concurrency. The right production
approach is a dedicated single-threaded worker process that owns all netCDF
calls, with the Rocket server forwarding merge jobs to it over a channel. That
decouples HTTP concurrency from the C library's limitations entirely.
