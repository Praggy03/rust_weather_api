# ── Stage 1: builder ─────────────────────────────────────────────────────────
# Bullseye ships HDF5 1.10.x, which is compatible with hdf5-sys 0.7.x
# (pulled in transitively by netcdf-sys 0.3.x).
# Bookworm ships HDF5 1.14.x which hdf5-sys 0.7.1 does not support.
FROM rust:bullseye AS builder

# libnetcdf-dev pulls in HDF5 and all other C-level build deps.
# pkg-config is required by the netcdf-sys build script.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libnetcdf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── cache dependency compilation separately from source changes ──────────────
# Copy manifests first; build a stub binary so cargo fetches+compiles all deps.
COPY Cargo.toml ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# ── build the real binary ────────────────────────────────────────────────────
COPY src ./src
# `touch` forces Cargo to recompile the crate even though mtime may be stale.
RUN touch src/main.rs src/netcdf_merge.rs \
    && cargo build --release

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
# Must match the builder OS so shared library SONAMEs align.
FROM debian:bullseye-slim AS runtime

# libnetcdf18  — NetCDF 4.7.x shared library on Bullseye.
# ca-certificates — useful if Rocket ever needs TLS outbound calls.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libnetcdf18 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/rust_weather_api ./rust_weather_api

# Rocket defaults to 127.0.0.1 — override so it's reachable inside a container.
ENV ROCKET_ADDRESS=0.0.0.0
ENV ROCKET_PORT=8000

EXPOSE 8000

CMD ["./rust_weather_api"]
