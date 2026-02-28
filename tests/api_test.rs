//! Integration tests — exercises every HTTP route via Rocket's blocking test client.
//!
//! Each test gets a fresh server instance (independent in-memory store).
//! All netCDF-C FFI calls in both the test helpers and the server-side merge
//! serialise through `rust_weather_api::netcdf_merge::NC_LOCK`.
//!
//! Run:  cargo test --test api_test -- --test-threads=1
//! (single-threaded is safest because HDF5 uses process-global state)

use rocket::http::{ContentType, Status};
use rocket::local::blocking::Client;
use rust_weather_api::{build_rocket, netcdf_merge::make_nc_bytes};

// ── helpers ───────────────────────────────────────────────────────────────────

fn client() -> Client {
    Client::tracked(build_rocket()).expect("valid Rocket instance")
}

fn upload(c: &Client, route: &str, name: &str, body: Vec<u8>) -> Status {
    c.post(format!("{route}?name={name}"))
        .header(ContentType::new("application", "octet-stream"))
        .body(body)
        .dispatch()
        .status()
}

fn read(c: &Client, name: &str) -> (Status, Vec<u8>) {
    let res = c.get(format!("/read?name={name}")).dispatch();
    let status = res.status();
    let body = res.into_bytes().unwrap_or_default();
    (status, body)
}

// ── upload tests ──────────────────────────────────────────────────────────────

#[test]
fn upload_part_a_returns_200() {
    let c = client();
    assert_eq!(upload(&c, "/part_a", "t", make_nc_bytes(&[], &[])), Status::Ok);
}

#[test]
fn upload_part_b_returns_200() {
    let c = client();
    assert_eq!(upload(&c, "/part_b", "t", make_nc_bytes(&[], &[])), Status::Ok);
}

// ── missing parts ─────────────────────────────────────────────────────────────

#[test]
fn read_with_no_uploads_returns_404_mentioning_part_a() {
    let c = client();
    let (status, body) = read(&c, "nobody");
    assert_eq!(status, Status::NotFound);
    assert!(
        body.windows(6).any(|w| w == b"part_a"),
        "body should mention part_a: {}",
        String::from_utf8_lossy(&body)
    );
}

#[test]
fn read_with_only_part_b_returns_404_mentioning_part_a() {
    let c = client();
    upload(&c, "/part_b", "only_b", make_nc_bytes(&[], &[]));
    let (status, body) = read(&c, "only_b");
    assert_eq!(status, Status::NotFound);
    assert!(body.windows(6).any(|w| w == b"part_a"));
}

#[test]
fn read_with_only_part_a_returns_404_mentioning_part_b() {
    let c = client();
    upload(&c, "/part_a", "only_a", make_nc_bytes(&[], &[]));
    let (status, body) = read(&c, "only_a");
    assert_eq!(status, Status::NotFound);
    assert!(body.windows(6).any(|w| w == b"part_b"));
}

// ── happy path ────────────────────────────────────────────────────────────────

#[test]
fn read_with_both_parts_returns_200_and_hdf5_magic() {
    let c = client();
    upload(&c, "/part_a", "full", make_nc_bytes(&[("x", 3)], &[("src", "A")]));
    upload(&c, "/part_b", "full", make_nc_bytes(&[("y", 4)], &[("src_b", "B")]));
    let (status, body) = read(&c, "full");
    assert_eq!(status, Status::Ok);
    assert!(
        body.starts_with(b"\x89HDF"),
        "expected HDF5 magic bytes, got: {:?}",
        &body[..8.min(body.len())]
    );
}

#[test]
fn same_dim_name_same_length_succeeds() {
    let c = client();
    upload(&c, "/part_a", "sameok", make_nc_bytes(&[("time", 10)], &[]));
    upload(&c, "/part_b", "sameok", make_nc_bytes(&[("time", 10)], &[]));
    let (status, body) = read(&c, "sameok");
    assert_eq!(status, Status::Ok);
    assert!(body.starts_with(b"\x89HDF"));
}

// ── error cases ───────────────────────────────────────────────────────────────

#[test]
fn dim_conflict_returns_400() {
    let c = client();
    upload(&c, "/part_a", "conflict", make_nc_bytes(&[("x", 3)], &[]));
    upload(&c, "/part_b", "conflict", make_nc_bytes(&[("x", 5)], &[])); // DIFFERENT LENGTH
    let (status, body) = read(&c, "conflict");
    assert_eq!(status, Status::BadRequest);
    let msg = String::from_utf8_lossy(&body);
    assert!(
        msg.contains("dimension conflict") || msg.contains('"') ,
        "body should describe the conflict: {msg}"
    );
}

#[test]
fn invalid_netcdf_bytes_returns_500() {
    let c = client();
    upload(&c, "/part_a", "bad", b"this is not netcdf".to_vec());
    upload(&c, "/part_b", "bad", b"also not netcdf".to_vec());
    let (status, _) = read(&c, "bad");
    assert_eq!(status, Status::InternalServerError);
}

// ── isolation & overwrite ─────────────────────────────────────────────────────

#[test]
fn names_are_isolated() {
    let c = client();
    upload(&c, "/part_a", "alpha", make_nc_bytes(&[], &[]));
    upload(&c, "/part_b", "alpha", make_nc_bytes(&[], &[]));

    let (alpha_status, _) = read(&c, "alpha");
    let (beta_status, _) = read(&c, "beta"); // never uploaded
    assert_eq!(alpha_status, Status::Ok);
    assert_eq!(beta_status, Status::NotFound);
}

#[test]
fn overwriting_part_a_replaces_stored_bytes() {
    let c = client();
    // Upload bad data first, verify it fails.
    upload(&c, "/part_a", "ow", b"junk".to_vec());
    upload(&c, "/part_b", "ow", make_nc_bytes(&[], &[]));
    let (s1, _) = read(&c, "ow");
    assert_eq!(s1, Status::InternalServerError);

    // Replace part_a with valid data — should now succeed.
    upload(&c, "/part_a", "ow", make_nc_bytes(&[], &[]));
    let (s2, body) = read(&c, "ow");
    assert_eq!(s2, Status::Ok);
    assert!(body.starts_with(b"\x89HDF"));
}
