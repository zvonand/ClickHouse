use std::ffi::{c_char, CString};
use std::panic;
use std::slice;

fn set_output(result: String, out: *mut *mut u8, out_size: *mut u64) {
    assert!(!out_size.is_null());
    let out_size_ptr = unsafe { &mut *out_size };
    *out_size_ptr = (result.len() + 1).try_into().unwrap();

    assert!(!out.is_null());
    let out_ptr = unsafe { &mut *out };
    *out_ptr = CString::new(result).unwrap().into_raw() as *mut u8;
}

/// Transpiles SQL from one dialect to ClickHouse SQL.
unsafe fn polyglot_transpile_impl(
    query: *const u8,
    query_size: u64,
    source_dialect: *const u8,
    source_dialect_size: u64,
    out: *mut *mut u8,
    out_size: *mut u64,
) -> i64 {
    let query_vec = slice::from_raw_parts(query, query_size.try_into().unwrap()).to_vec();
    let Ok(query_str) = String::from_utf8(query_vec) else {
        set_output(
            "The query must be UTF-8 encoded!".to_string(),
            out,
            out_size,
        );
        return 1;
    };

    let dialect_vec =
        slice::from_raw_parts(source_dialect, source_dialect_size.try_into().unwrap()).to_vec();
    let Ok(dialect_str) = String::from_utf8(dialect_vec) else {
        set_output(
            "The dialect name must be UTF-8 encoded!".to_string(),
            out,
            out_size,
        );
        return 1;
    };

    match polyglot_sql::transpile_by_name(&query_str, &dialect_str, "clickhouse") {
        Ok(statements) if statements.len() == 1 => {
            set_output(statements.into_iter().next().unwrap(), out, out_size);
            0
        }
        Ok(statements) if statements.is_empty() => {
            set_output(
                "Polyglot transpilation returned no statements".to_string(),
                out,
                out_size,
            );
            1
        }
        Ok(_) => {
            set_output(
                "Polyglot transpilation returned multiple statements, but only single-statement queries are supported".to_string(),
                out,
                out_size,
            );
            1
        }
        Err(e) => {
            set_output(format!("Polyglot transpilation failed: {e}"), out, out_size);
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn polyglot_transpile(
    query: *const u8,
    query_size: u64,
    source_dialect: *const u8,
    source_dialect_size: u64,
    out: *mut *mut u8,
    out_size: *mut u64,
) -> i64 {
    panic::catch_unwind(|| {
        polyglot_transpile_impl(query, query_size, source_dialect, source_dialect_size, out, out_size)
    })
    .unwrap_or_else(|_| {
        set_output("polyglot panicked".to_string(), out, out_size);
        1
    })
}

#[no_mangle]
pub unsafe extern "C" fn polyglot_free_pointer(ptr_to_free: *mut u8) {
    if !ptr_to_free.is_null() {
        std::mem::drop(CString::from_raw(ptr_to_free as *mut c_char));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{CStr, CString};

    fn run_transpile(query: &str, dialect: &str) -> (String, i64) {
        let query_cstr = CString::new(query).unwrap();
        let query_ptr = query_cstr.as_ptr() as *const u8;
        let query_size = query_cstr.to_bytes().len() as u64;

        let dialect_cstr = CString::new(dialect).unwrap();
        let dialect_ptr = dialect_cstr.as_ptr() as *const u8;
        let dialect_size = dialect_cstr.to_bytes().len() as u64;

        let mut out: *mut u8 = std::ptr::null_mut();
        let mut out_size = 0_u64;

        unsafe {
            let success = polyglot_transpile(
                query_ptr,
                query_size,
                dialect_ptr,
                dialect_size,
                &mut out,
                &mut out_size,
            );
            let output = CStr::from_ptr(out as *const i8)
                .to_str()
                .unwrap()
                .to_string();
            polyglot_free_pointer(out);
            (output, success)
        }
    }

    #[test]
    fn test_transpile_sqlite_to_clickhouse() {
        let (result, code) = run_transpile("SELECT IFNULL(a, 1) FROM t", "sqlite");
        assert_eq!(code, 0, "Transpilation failed: {result}");
        assert!(
            result.to_uppercase().contains("SELECT"),
            "Expected SELECT in output: {result}"
        );
    }

    #[test]
    fn test_invalid_dialect() {
        let (_result, code) = run_transpile("SELECT 1", "not_a_real_dialect");
        assert_eq!(code, 1);
    }
}
