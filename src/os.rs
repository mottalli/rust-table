/// Wrappers for OS (POSIX) functions
use libc::{c_char, c_void, free};
use std::ffi::{CString, CStr};
use std::path::PathBuf;

extern {
    fn tempnam(dir: *const c_char, prefix: *const c_char) -> *mut c_char;
}

pub fn tempname(prefix: &str) -> PathBuf {
    let temp_dir = CString::new("/tmp").unwrap();
    let prefix_ptr = CString::new(prefix).expect("tempname prefix contains non-UTF8 chars").as_ptr();

    unsafe {
        let buffer = tempnam(temp_dir.as_ptr(), prefix_ptr);
        let path_name = CStr::from_ptr(buffer).to_str().unwrap();

        let mut full_path = PathBuf::from(temp_dir.to_str().unwrap());
        full_path.push(path_name);
        free(buffer as *mut c_void);

        full_path
    }
}
