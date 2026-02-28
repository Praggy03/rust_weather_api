//! In-memory NetCDF merge using raw netcdf-sys FFI.
//!
//! All netCDF-C calls are serialised through `NETCDF_LOCK`, a process-global
//! Mutex<()>, so that the HDF5/netCDF global C state is never accessed
//! concurrently.  (`netcdf-sys` provides only raw FFI; the lock lives here.)

use libc::{c_char, c_int, c_void, size_t};
use netcdf_sys::*;
use std::ffi::{CStr, CString};
use thiserror::Error;

// ── in-memory FFI not yet exposed by netcdf-sys 0.3.x ────────────────────────
// These symbols exist in libnetcdf >= 4.6 but netcdf-sys 0.3 does not bind them.
// Declaring them here is safe because the crate already links against libnetcdf.

#[repr(C)]
struct NC_memio {
    size: size_t,
    memory: *mut c_void,
    flags: c_int,
}

extern "C" {
    fn nc_open_mem(
        path: *const c_char,
        mode: c_int,
        size: size_t,
        memory: *mut c_void,
        ncidp: *mut c_int,
    ) -> c_int;

    fn nc_create_mem(
        path: *const c_char,
        cmode: c_int,
        initialsize: size_t,
        ncidp: *mut c_int,
    ) -> c_int;

    fn nc_close_memio(ncid: c_int, info: *mut NC_memio) -> c_int;
}

/// Process-global lock that serialises every netCDF-C / HDF5 call.
/// Exposed as `pub` so integration tests share the same lock and never
/// race with server-side merges.
pub static NC_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

// ── error type ───────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum MergeError {
    #[error("netCDF error {code}: {msg}")]
    Nc { code: i32, msg: String },

    #[error("dimension conflict: '{name}' has length {a} in part_a but {b} in part_b")]
    DimConflict { name: String, a: usize, b: usize },

    #[error("unlimited dimension conflict: '{name}' is unlimited in one file but fixed in the other")]
    UnlimitedConflict { name: String },

    #[error("unsupported variable type/UDT for variable '{var}' — nc_copy_var returned {code}")]
    UnsupportedVar { var: String, code: i32 },

    #[error("internal error: {0}")]
    Internal(String),
}

impl MergeError {
    fn nc(code: c_int) -> Self {
        let msg = unsafe {
            let ptr = nc_strerror(code);
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        };
        MergeError::Nc { code, msg }
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Unwrap a netCDF return code or convert to MergeError.
macro_rules! nc_try {
    ($expr:expr) => {{
        let rc = $expr as c_int;
        if rc != NC_NOERR as c_int {
            return Err(MergeError::nc(rc));
        }
        rc
    }};
}

/// `nc_open_mem` wrapper — opens a NetCDF dataset from a byte slice.
/// The slice must outlive the returned ncid.
unsafe fn open_mem(bytes: &[u8]) -> Result<c_int, MergeError> {
    let mut ncid: c_int = 0;
    nc_try!(nc_open_mem(
        b"memory\0".as_ptr() as *const c_char,
        NC_NOWRITE as c_int,
        bytes.len() as size_t,
        bytes.as_ptr() as *mut c_void,
        &mut ncid,
    ));
    Ok(ncid)
}

/// Query every dimension name+length and whether it is unlimited.
unsafe fn query_dims(ncid: c_int) -> Result<Vec<(String, usize, bool)>, MergeError> {
    let mut ndims: c_int = 0;
    let mut nunlim: c_int = 0;
    let mut nvars: c_int = 0;
    let mut natts: c_int = 0;
    let mut unlimdimid: c_int = 0;
    nc_try!(nc_inq(ncid, &mut ndims, &mut nvars, &mut natts, &mut unlimdimid));

    // Collect all unlimited dim ids (netCDF-4 can have multiple)
    let mut unlim_ids: Vec<c_int> = vec![0; (ndims + 1) as usize];
    let rc = nc_inq_unlimdims(ncid, &mut nunlim, unlim_ids.as_mut_ptr());
    let unlimited_set: std::collections::HashSet<c_int> = if rc == NC_NOERR as c_int {
        unlim_ids[..nunlim as usize].iter().copied().collect()
    } else {
        // fall back: only the single classic unlimited dim
        if unlimdimid >= 0 {
            std::iter::once(unlimdimid).collect()
        } else {
            Default::default()
        }
    };

    let mut dims = Vec::with_capacity(ndims as usize);
    for i in 0..ndims {
        let mut name_buf: [c_char; NC_MAX_NAME as usize + 1] = [0; NC_MAX_NAME as usize + 1];
        let mut len: size_t = 0;
        nc_try!(nc_inq_dim(ncid, i, name_buf.as_mut_ptr(), &mut len));
        let name = CStr::from_ptr(name_buf.as_ptr())
            .to_string_lossy()
            .into_owned();
        dims.push((name, len, unlimited_set.contains(&i)));
    }
    Ok(dims)
}

// ── public entry point ────────────────────────────────────────────────────────

/// Merge two in-memory NetCDF byte buffers into a third.
/// All netCDF-C library calls are serialised via `NETCDF_LOCK`.
pub fn merge(part_a: &[u8], part_b: &[u8]) -> Result<Vec<u8>, MergeError> {
    // Acquire the global netCDF/HDF5 lock for the entire operation.
    let _guard = NC_LOCK
        .lock()
        .map_err(|e| MergeError::Internal(e.to_string()))?;

    unsafe { merge_inner(part_a, part_b) }
}

/// Inner merge logic — called with the global lock already held.
unsafe fn merge_inner(part_a: &[u8], part_b: &[u8]) -> Result<Vec<u8>, MergeError> {
    // ── 1. Open both inputs ──────────────────────────────────────────────────
    let ncid_a = open_mem(part_a)?;
    let ncid_b = open_mem(part_b)?;

    // ── 2. Query dimensions from both ────────────────────────────────────────
    let dims_a = query_dims(ncid_a)?;
    let dims_b = query_dims(ncid_b)?;

    // Build merged dimension list preserving insertion order.
    // name → (len, is_unlimited).  Conflict = same name, different value → 400.
    let mut dim_vec: Vec<(String, usize, bool)> = Vec::new();
    let mut dim_seen: std::collections::HashMap<String, (usize, bool)> =
        std::collections::HashMap::new();

    for (name, len, unlim) in dims_a.iter().chain(dims_b.iter()) {
        if let Some(&(existing_len, existing_unlim)) = dim_seen.get(name.as_str()) {
            if existing_len != *len {
                nc_close(ncid_a);
                nc_close(ncid_b);
                return Err(MergeError::DimConflict {
                    name: name.clone(),
                    a: existing_len,
                    b: *len,
                });
            }
            if existing_unlim != *unlim {
                nc_close(ncid_a);
                nc_close(ncid_b);
                return Err(MergeError::UnlimitedConflict { name: name.clone() });
            }
        } else {
            dim_seen.insert(name.clone(), (*len, *unlim));
            dim_vec.push((name.clone(), *len, *unlim));
        }
    }

    // ── 3. Create output in memory (netCDF-4) ────────────────────────────────
    // nc_create_mem: name is ignored (in-memory), initial size hint 64 KiB.
    let mut ncid_out: c_int = 0;
    let create_mode = (NC_NETCDF4 | NC_CLOBBER) as c_int;
    let rc = nc_create_mem(
        b"output\0".as_ptr() as *const c_char,
        create_mode,
        64 * 1024, // initial memory size hint
        &mut ncid_out,
    );
    if rc != NC_NOERR as c_int {
        nc_close(ncid_a);
        nc_close(ncid_b);
        return Err(MergeError::nc(rc));
    }

    // From here all early exits must close ncid_a, ncid_b, ncid_out.
    // We use a helper closure that returns Result, then clean up after.
    let result = merge_with_handles(ncid_a, ncid_b, ncid_out, &dim_vec);

    if result.is_err() {
        // nc_close_memio was not called — close ncid_out explicitly.
        nc_close(ncid_out);
    }
    nc_close(ncid_a);
    nc_close(ncid_b);
    result
}

/// Does the heavy lifting once all three file handles are open.
/// On success returns the merged bytes (ncid_out has been closed via nc_close_memio).
/// On failure returns an error (caller must close ncid_out, ncid_a, ncid_b).
unsafe fn merge_with_handles(
    ncid_a: c_int,
    ncid_b: c_int,
    ncid_out: c_int,
    dim_vec: &[(String, usize, bool)],
) -> Result<Vec<u8>, MergeError> {
    // ── 4. Define dimensions in output ───────────────────────────────────────
    for (name, len, unlim) in dim_vec {
        let c_name = std::ffi::CString::new(name.as_str())
            .map_err(|e| MergeError::Internal(e.to_string()))?;
        let mut dimid_out: c_int = 0;
        let dim_len: size_t = if *unlim { NC_UNLIMITED as size_t } else { *len };
        nc_try!(nc_def_dim(ncid_out, c_name.as_ptr(), dim_len, &mut dimid_out));
    }

    // ── 5. Copy global attributes (part_a wins on conflict) ──────────────────
    copy_global_attrs(ncid_a, ncid_out)?;
    copy_global_attrs_if_missing(ncid_b, ncid_out)?;

    // ── 6. Copy variables (part_a wins on conflict) ──────────────────────────
    copy_vars(ncid_a, ncid_out)?;
    copy_vars_if_missing(ncid_b, ncid_out)?;

    // ── 7. Close output and extract bytes ────────────────────────────────────
    let mut memio = NC_memio {
        size: 0,
        memory: std::ptr::null_mut(),
        flags: 0,
    };
    nc_try!(nc_close_memio(ncid_out, &mut memio));

    // The caller owns the buffer after nc_close_memio; always free with libc::free.
    let bytes = if memio.memory.is_null() || memio.size == 0 {
        Vec::new()
    } else {
        let slice = std::slice::from_raw_parts(memio.memory as *const u8, memio.size);
        let v = slice.to_vec();
        libc::free(memio.memory);
        v
    };

    Ok(bytes)
}

// ── attribute helpers ─────────────────────────────────────────────────────────

/// Copy every global attribute from `src` into `dst`.
unsafe fn copy_global_attrs(src: c_int, dst: c_int) -> Result<(), MergeError> {
    let mut natts: c_int = 0;
    nc_try!(nc_inq_natts(src, &mut natts));
    for i in 0..natts {
        let mut name_buf: [c_char; NC_MAX_NAME as usize + 1] = [0; NC_MAX_NAME as usize + 1];
        nc_try!(nc_inq_attname(src, NC_GLOBAL, i, name_buf.as_mut_ptr()));
        nc_try!(nc_copy_att(src, NC_GLOBAL, name_buf.as_ptr(), dst, NC_GLOBAL));
    }
    Ok(())
}

/// Copy global attributes from `src` into `dst` only if they don't exist yet.
unsafe fn copy_global_attrs_if_missing(src: c_int, dst: c_int) -> Result<(), MergeError> {
    let mut natts: c_int = 0;
    nc_try!(nc_inq_natts(src, &mut natts));
    for i in 0..natts {
        let mut name_buf: [c_char; NC_MAX_NAME as usize + 1] = [0; NC_MAX_NAME as usize + 1];
        nc_try!(nc_inq_attname(src, NC_GLOBAL, i, name_buf.as_mut_ptr()));
        // Check whether already present in dst
        let mut attid: c_int = 0;
        let rc = nc_inq_attid(dst, NC_GLOBAL, name_buf.as_ptr(), &mut attid);
        if rc == NC_ENOTATT as c_int {
            nc_try!(nc_copy_att(src, NC_GLOBAL, name_buf.as_ptr(), dst, NC_GLOBAL));
        }
        // If rc == NC_NOERR the attribute already exists (from part_a) — skip.
        // Any other error is ignored for attributes (non-fatal).
    }
    Ok(())
}

// ── variable helpers ──────────────────────────────────────────────────────────

/// Copy all variables from `src` into `dst`.
/// `nc_copy_var` resolves dimension names in `dst` automatically.
unsafe fn copy_vars(src: c_int, dst: c_int) -> Result<(), MergeError> {
    let mut nvars: c_int = 0;
    let mut d0: c_int = 0;
    let mut d1: c_int = 0;
    let mut d2: c_int = 0;
    nc_try!(nc_inq(src, &mut d0, &mut nvars, &mut d1, &mut d2));

    for varid in 0..nvars {
        let mut name_buf: [c_char; NC_MAX_NAME as usize + 1] = [0; NC_MAX_NAME as usize + 1];
        nc_try!(nc_inq_varname(src, varid, name_buf.as_mut_ptr()));
        let var_name = CStr::from_ptr(name_buf.as_ptr())
            .to_string_lossy()
            .into_owned();

        let rc = do_copy_var(src, varid, dst);
        if rc != NC_NOERR as c_int {
            return Err(MergeError::UnsupportedVar {
                var: var_name,
                code: rc,
            });
        }
    }
    Ok(())
}

/// Copy variables from `src` into `dst` only if they don't exist yet.
unsafe fn copy_vars_if_missing(src: c_int, dst: c_int) -> Result<(), MergeError> {
    let mut nvars: c_int = 0;
    let mut d0: c_int = 0;
    let mut d1: c_int = 0;
    let mut d2: c_int = 0;
    nc_try!(nc_inq(src, &mut d0, &mut nvars, &mut d1, &mut d2));

    for varid in 0..nvars {
        let mut name_buf: [c_char; NC_MAX_NAME as usize + 1] = [0; NC_MAX_NAME as usize + 1];
        nc_try!(nc_inq_varname(src, varid, name_buf.as_mut_ptr()));
        let var_name = CStr::from_ptr(name_buf.as_ptr())
            .to_string_lossy()
            .into_owned();

        // Check if already present in output (part_a wins)
        let mut out_varid: c_int = 0;
        let check = nc_inq_varid(dst, name_buf.as_ptr(), &mut out_varid);
        if check == NC_NOERR as c_int {
            continue; // already present from part_a
        }

        let rc = do_copy_var(src, varid, dst);
        if rc != NC_NOERR as c_int {
            return Err(MergeError::UnsupportedVar {
                var: var_name,
                code: rc,
            });
        }
    }
    Ok(())
}

/// Call `nc_copy_var` and return the raw return code (caller handles errors).
/// `nc_copy_var` resolves dimension names internally in the target file.
unsafe fn do_copy_var(src: c_int, varid: c_int, dst: c_int) -> c_int {
    nc_copy_var(src, varid, dst) as c_int
}

// ── test helper ───────────────────────────────────────────────────────────────

/// Creates a minimal in-memory NetCDF-4 file with the given dimensions and
/// global text attributes.  Intended for testing; **panics** on any netCDF error.
///
/// * `dims`  — `(name, length)` pairs; all are fixed (non-unlimited).
/// * `attrs` — `(name, value)` global text attributes.
pub fn make_nc_bytes(dims: &[(&str, usize)], attrs: &[(&str, &str)]) -> Vec<u8> {
    let _guard = NC_LOCK.lock().expect("NC_LOCK poisoned");
    unsafe {
        let mut ncid: c_int = 0;
        let rc = nc_create_mem(
            b"test\0".as_ptr() as *const c_char,
            (NC_NETCDF4 | NC_CLOBBER) as c_int,
            64 * 1024,
            &mut ncid,
        );
        assert_eq!(rc, NC_NOERR as c_int, "nc_create_mem failed: {rc}");

        for (name, len) in dims {
            let cname = CString::new(*name).unwrap();
            let mut dimid: c_int = 0;
            let rc = nc_def_dim(ncid, cname.as_ptr(), *len as size_t, &mut dimid);
            assert_eq!(rc, NC_NOERR as c_int, "nc_def_dim '{name}' failed: {rc}");
        }

        for (aname, aval) in attrs {
            let cname = CString::new(*aname).unwrap();
            let val = aval.as_bytes();
            let rc = nc_put_att_text(
                ncid,
                NC_GLOBAL,
                cname.as_ptr(),
                val.len() as size_t,
                val.as_ptr() as *const c_char,
            );
            assert_eq!(rc, NC_NOERR as c_int, "nc_put_att_text '{aname}' failed: {rc}");
        }

        let mut memio = NC_memio { size: 0, memory: std::ptr::null_mut(), flags: 0 };
        let rc = nc_close_memio(ncid, &mut memio);
        assert_eq!(rc, NC_NOERR as c_int, "nc_close_memio failed: {rc}");

        let v = std::slice::from_raw_parts(memio.memory as *const u8, memio.size).to_vec();
        libc::free(memio.memory);
        v
    }
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{merge, make_nc_bytes, NC_LOCK, MergeError};
    use netcdf_sys::{NC_GLOBAL, NC_NOWRITE, nc_close, nc_inq_att, nc_get_att_text};
    use libc::{c_char, c_void, size_t};
    use std::ffi::CString;

    #[test]
    fn merge_two_empty_files() {
        let a = make_nc_bytes(&[], &[]);
        let b = make_nc_bytes(&[], &[]);
        let out = merge(&a, &b).expect("empty merge should succeed");
        assert!(out.starts_with(b"\x89HDF"), "result must be HDF5/NetCDF-4");
    }

    #[test]
    fn merge_different_dims_and_attrs() {
        let a = make_nc_bytes(&[("x", 3)], &[("source", "A")]);
        let b = make_nc_bytes(&[("y", 4)], &[("source_b", "B")]);
        let out = merge(&a, &b).expect("merge with different dims");
        assert!(out.starts_with(b"\x89HDF"));
    }

    #[test]
    fn part_a_wins_on_attr_conflict() {
        let a = make_nc_bytes(&[], &[("title", "from_a")]);
        let b = make_nc_bytes(&[], &[("title", "from_b")]);
        let merged = merge(&a, &b).expect("merge with conflicting attr should succeed");

        // Re-open the merged file and read the attribute value.
        let _guard = NC_LOCK.lock().unwrap();
        unsafe {
            let mut ncid: i32 = 0;
            super::nc_open_mem(
                b"verify\0".as_ptr() as *const c_char,
                NC_NOWRITE as i32,
                merged.len() as size_t,
                merged.as_ptr() as *mut c_void,
                &mut ncid,
            );
            let cname = CString::new("title").unwrap();
            let mut att_len: size_t = 0;
            nc_inq_att(ncid, NC_GLOBAL, cname.as_ptr(), std::ptr::null_mut(), &mut att_len);
            let mut buf = vec![0u8; att_len];
            nc_get_att_text(ncid, NC_GLOBAL, cname.as_ptr(), buf.as_mut_ptr() as *mut c_char);
            nc_close(ncid);
            assert_eq!(std::str::from_utf8(&buf).unwrap(), "from_a");
        }
    }

    #[test]
    fn same_dim_same_length_succeeds() {
        // Same name + same length → no conflict; dimension deduped in output.
        let a = make_nc_bytes(&[("time", 10)], &[]);
        let b = make_nc_bytes(&[("time", 10)], &[]);
        let out = merge(&a, &b).expect("identical dim should succeed");
        assert!(out.starts_with(b"\x89HDF"));
    }

    #[test]
    fn dim_length_conflict_returns_error() {
        let a = make_nc_bytes(&[("x", 3)], &[]);
        let b = make_nc_bytes(&[("x", 5)], &[]); // same name, different length
        let err = merge(&a, &b).expect_err("dim conflict should fail");
        assert!(matches!(err, MergeError::DimConflict { .. }), "got: {err}");
    }

    #[test]
    fn invalid_bytes_part_a_returns_nc_error() {
        let bad = b"this is not a netcdf file";
        let b = make_nc_bytes(&[], &[]);
        let err = merge(bad, &b).expect_err("bad part_a should fail");
        assert!(matches!(err, MergeError::Nc { .. }), "got: {err}");
    }

    #[test]
    fn invalid_bytes_part_b_returns_nc_error() {
        let a = make_nc_bytes(&[], &[]);
        let bad = b"not netcdf either";
        let err = merge(&a, bad).expect_err("bad part_b should fail");
        assert!(matches!(err, MergeError::Nc { .. }), "got: {err}");
    }
}
