#!/usr/bin/env python3
"""End-to-end tests for the netcdf-merge-server container.

Usage:
    # Start the server first (e.g. via scripts/e2e_test.sh), then:
    python3 scripts/e2e_test.py [base_url]

    # base_url defaults to http://localhost:8000 or the BASE_URL env var.

No external Python packages required — stdlib only.
"""

import os
import sys
import time
import urllib.request
import urllib.error

BASE_URL = (
    sys.argv[1]
    if len(sys.argv) > 1
    else os.environ.get("BASE_URL", "http://localhost:8000")
)

PASS = FAIL = 0


def check(name: str, actual, expected):
    global PASS, FAIL
    if actual == expected:
        print(f"  PASS  {name}")
        PASS += 1
    else:
        print(f"  FAIL  {name}")
        print(f"         expected: {expected!r}")
        print(f"         got:      {actual!r}")
        FAIL += 1


def post(path: str, data: bytes) -> int:
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        method="POST",
    )
    try:
        return urllib.request.urlopen(req).status
    except urllib.error.HTTPError as e:
        return e.code


def get(path: str):
    try:
        r = urllib.request.urlopen(f"{BASE_URL}{path}")
        return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


# ── minimal valid NetCDF-3 Classic file (32 bytes) ────────────────────────────
# Format: magic(4) + numrecs=0(4) + ABSENT×3 (each ABSENT = two 4-byte ZEROs)
EMPTY_NC3 = b"CDF\x01" + b"\x00" * 28  # 32 bytes — no dims, no attrs, no vars

# ── wait for server ───────────────────────────────────────────────────────────
print(f"Target: {BASE_URL}\n")
print("Waiting for server to start...")
for attempt in range(30):
    try:
        urllib.request.urlopen(f"{BASE_URL}/read?name=__ping__")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            break  # server is up (correct 404 response)
    except Exception:
        pass
    time.sleep(0.5)
else:
    print("ERROR: server did not respond within 15 s")
    sys.exit(1)
print("Server is up.\n")

# ── 1. missing data ───────────────────────────────────────────────────────────
print("── 1. Missing data ──")
status, body = get("/read?name=nobody")
check("GET /read (no uploads) → 404", status, 404)
check("body mentions 'part_a'", b"part_a" in body, True)

# ── 2. upload ─────────────────────────────────────────────────────────────────
print("\n── 2. Upload ──")
check("POST /part_a → 200", post("/part_a?name=t1", EMPTY_NC3), 200)
check("POST /part_b → 200", post("/part_b?name=t1", EMPTY_NC3), 200)

# ── 3. merge & read ───────────────────────────────────────────────────────────
print("\n── 3. Merge & read ──")
status, body = get("/read?name=t1")
check("GET /read → 200", status, 200)
check("response is non-empty", len(body) > 0, True)
check("response is HDF5/NetCDF-4 (\\x89HDF magic)", body[:4], b"\x89HDF")

# ── 4. missing part_a ────────────────────────────────────────────────────────
print("\n── 4. Missing part_a ──")
check("POST /part_b only → 200", post("/part_b?name=t2", EMPTY_NC3), 200)
status, body = get("/read?name=t2")
check("GET /read (only part_b) → 404", status, 404)
check("body mentions 'part_a'", b"part_a" in body, True)

# ── 5. missing part_b ────────────────────────────────────────────────────────
print("\n── 5. Missing part_b ──")
check("POST /part_a only → 200", post("/part_a?name=t3", EMPTY_NC3), 200)
status, body = get("/read?name=t3")
check("GET /read (only part_a) → 404", status, 404)
check("body mentions 'part_b'", b"part_b" in body, True)

# ── 6. name isolation ─────────────────────────────────────────────────────────
print("\n── 6. Name isolation ──")
check("POST /part_a t4 → 200", post("/part_a?name=t4", EMPTY_NC3), 200)
check("POST /part_b t4 → 200", post("/part_b?name=t4", EMPTY_NC3), 200)
s4, _ = get("/read?name=t4")
s5, _ = get("/read?name=t5")  # never uploaded
check("t4 (both uploaded) → 200", s4, 200)
check("t5 (not uploaded)  → 404", s5, 404)

# ── 7. invalid NetCDF bytes → server error ────────────────────────────────────
print("\n── 7. Invalid NetCDF bytes ──")
check("POST /part_a bad bytes → 200", post("/part_a?name=bad", b"not netcdf"), 200)
check("POST /part_b bad bytes → 200", post("/part_b?name=bad", b"also not netcdf"), 200)
status, _ = get("/read?name=bad")
check("GET /read bad data → 500", status, 500)

# ── 8. overwrite: re-uploading valid data fixes a bad upload ─────────────────
print("\n── 8. Overwrite ──")
check("POST /part_a junk → 200",  post("/part_a?name=ow", b"junk"), 200)
check("POST /part_b valid → 200", post("/part_b?name=ow", EMPTY_NC3), 200)
s_bad, _ = get("/read?name=ow")
check("GET /read (part_a=junk) → 500", s_bad, 500)
check("POST /part_a fix → 200",   post("/part_a?name=ow", EMPTY_NC3), 200)
s_ok, body_ok = get("/read?name=ow")
check("GET /read (fixed) → 200",  s_ok, 200)
check("fixed result is HDF5",     body_ok[:4], b"\x89HDF")

# ── summary ───────────────────────────────────────────────────────────────────
print(f"\n{'─' * 50}")
print(f"  {PASS} passed   {FAIL} failed")
if FAIL:
    sys.exit(1)
print("  All E2E tests passed! ✓")
