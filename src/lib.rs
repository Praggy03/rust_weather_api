//! Rocket HTTP server — in-memory NetCDF file merging.
//!
//! Routes
//!   POST /part_a?name=NAME   — store part_a bytes
//!   POST /part_b?name=NAME   — store part_b bytes
//!   GET  /read?name=NAME     — merge and return bytes

#[macro_use]
extern crate rocket;

pub mod netcdf_merge;

use netcdf_merge::MergeError;
use rocket::{
    data::{Data, ToByteUnit},
    http::{ContentType, Status},
    response::{self, Responder, Response},
    Request, State,
};
use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, RwLock},
};
use thiserror::Error;

// ── constants ─────────────────────────────────────────────────────────────────

/// Maximum upload size per part (256 MiB).
pub const MAX_UPLOAD: u64 = 256 * 1024 * 1024;

// ── shared state ──────────────────────────────────────────────────────────────

/// One entry per `name` key.
#[derive(Default, Clone)]
struct Entry {
    part_a: Option<Arc<Vec<u8>>>,
    part_b: Option<Arc<Vec<u8>>>,
}

pub(crate) type Store = Arc<RwLock<HashMap<String, Entry>>>;

// ── error type ────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
enum ApiError {
    #[error("part_a missing for '{0}'")]
    MissingPartA(String),
    #[error("part_b missing for '{0}'")]
    MissingPartB(String),
    #[error("merge failed: {0}")]
    Merge(#[from] MergeError),
    #[error("upload too large (max 256 MiB)")]
    TooLarge,
    #[error("internal error: {0}")]
    Internal(String),
}

impl ApiError {
    fn status(&self) -> Status {
        match self {
            ApiError::MissingPartA(_) | ApiError::MissingPartB(_) => Status::NotFound,
            ApiError::Merge(MergeError::DimConflict { .. })
            | ApiError::Merge(MergeError::UnlimitedConflict { .. })
            | ApiError::Merge(MergeError::UnsupportedVar { .. }) => Status::BadRequest,
            ApiError::TooLarge => Status::PayloadTooLarge,
            _ => Status::InternalServerError,
        }
    }
}

impl<'r, 'o: 'r> Responder<'r, 'o> for ApiError {
    fn respond_to(self, _req: &'r Request<'_>) -> response::Result<'o> {
        let body = self.to_string();
        Response::build()
            .status(self.status())
            .header(ContentType::Plain)
            .sized_body(body.len(), Cursor::new(body))
            .ok()
    }
}

// ── routes ────────────────────────────────────────────────────────────────────

/// GET /health — liveness probe
#[get("/health")]
fn health() -> &'static str {
    "ok"
}

/// POST /part_a?name=NAME
#[post("/part_a?<name>", data = "<data>")]
async fn upload_part_a(
    name: &str,
    data: Data<'_>,
    store: &State<Store>,
) -> Result<(Status, &'static str), ApiError> {
    let bytes = read_body(data).await?;
    let mut map = store.write().map_err(|e| ApiError::Internal(e.to_string()))?;
    let entry = map.entry(name.to_string()).or_default();
    entry.part_a = Some(Arc::new(bytes));
    Ok((Status::Ok, "stored part_a"))
}

/// POST /part_b?name=NAME
#[post("/part_b?<name>", data = "<data>")]
async fn upload_part_b(
    name: &str,
    data: Data<'_>,
    store: &State<Store>,
) -> Result<(Status, &'static str), ApiError> {
    let bytes = read_body(data).await?;
    let mut map = store.write().map_err(|e| ApiError::Internal(e.to_string()))?;
    let entry = map.entry(name.to_string()).or_default();
    entry.part_b = Some(Arc::new(bytes));
    Ok((Status::Ok, "stored part_b"))
}

/// GET /read?name=NAME
#[get("/read?<name>")]
async fn read(
    name: &str,
    store: &State<Store>,
) -> Result<(Status, (ContentType, Vec<u8>)), ApiError> {
    // Snapshot both Arcs under a short-lived read lock, then drop it.
    let (part_a, part_b) = {
        let map = store.read().map_err(|e| ApiError::Internal(e.to_string()))?;
        let entry = map.get(name).cloned().unwrap_or_default();
        (entry.part_a, entry.part_b)
    };

    let part_a = part_a.ok_or_else(|| ApiError::MissingPartA(name.to_string()))?;
    let part_b = part_b.ok_or_else(|| ApiError::MissingPartB(name.to_string()))?;

    // Run the CPU-heavy merge on a blocking thread so the async runtime stays
    // responsive.  The global netCDF lock inside `merge` serialises library access.
    let merged = rocket::tokio::task::spawn_blocking(move || {
        netcdf_merge::merge(&part_a, &part_b)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))?
    .map_err(ApiError::Merge)?;

    Ok((Status::Ok, (ContentType::Binary, merged)))
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Read the request body up to `MAX_UPLOAD` bytes.
async fn read_body(data: Data<'_>) -> Result<Vec<u8>, ApiError> {
    data.open(MAX_UPLOAD.bytes())
        .into_bytes()
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))
        .and_then(|b| {
            if b.is_complete() {
                Ok(b.into_inner())
            } else {
                Err(ApiError::TooLarge)
            }
        })
}

// ── build ─────────────────────────────────────────────────────────────────────

/// Build the configured Rocket instance.  Called by `main` and by tests.
pub fn build_rocket() -> rocket::Rocket<rocket::Build> {
    let store: Store = Arc::new(RwLock::new(HashMap::new()));

    // Use figment as the base so ROCKET_ADDRESS / ROCKET_PORT env vars are
    // respected, then overlay our custom upload size limits on top.
    rocket::build()
        .manage(store)
        .configure(
            rocket::Config::figment()
                .merge(("limits.bytes", MAX_UPLOAD))
                .merge(("limits.data-form", MAX_UPLOAD)),
        )
        .mount("/", routes![health, upload_part_a, upload_part_b, read])
}
