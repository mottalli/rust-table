extern crate libc;
#[cfg(test)]

use table;
use std::path::PathBuf;
use std::ptr;
use std::ffi::{CStr, CString};
use std::fs;
use std::os::raw::{c_char, c_void};

use self::libc::{free};


/// Generates a path for testing. Gets deleted when
/// the test finishes.
extern {
    fn tempnam(dir: *const c_char, prefix: *const c_char) -> *mut c_char;
}

struct TestPath {
    path: PathBuf
}

impl TestPath {
    fn new() -> TestPath {

        let path_name: PathBuf = unsafe {
            let mut tmp_path = PathBuf::from("/tmp");
            let buffer = tempnam(CString::new(tmp_path.to_str().unwrap()).unwrap().as_ptr(), ptr::null_mut());
            let path_name = CStr::from_ptr(buffer).to_str().expect("Temp path name contains non-UTF8 chars");
            tmp_path.push(path_name);
            free(buffer as *mut self::libc::c_void);
            tmp_path
        };
        fs::create_dir(path_name.as_path()).unwrap();

        TestPath {
            path: path_name
        }
    }

    fn file_name(&self, name: &str) -> PathBuf {
        let mut tmp = self.path.clone();
        tmp.push(name);
        tmp
    }
}

impl Drop for TestPath {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).unwrap();
    }
}

#[test]
fn table_can_be_built() {
    let test_path = TestPath::new();

    let table = table::Table::build("test")
        .column("id", table::ColumnDatatype::Int32)
        .at(test_path.file_name("test.table")).unwrap();
}

#[test]
fn table_generates_right_columns() {
    let test_path = TestPath::new();

    let table = table::Table::build("test")
        .column("col1", table::ColumnDatatype::Int32)
        .column("col2", table::ColumnDatatype::Int32)
        .at(test_path.file_name("test.table")).unwrap();
}

#[test]
fn table_builder_in_invalid_path() {
    let builder = table::Table::build("test") ;
    assert!(builder.at("/invalid/path/test.table").is_err());
    assert!(builder.at("/tmp").is_err());
    assert!(builder.at("/").is_err());
    assert!(builder.at("").is_err());
}

#[test]
#[should_panic(expected="more than once")]
fn table_with_duplicated_columns() {
    let table = table::Table::build("test")
        .column("id", table::ColumnDatatype::Int32)
        .column("id", table::ColumnDatatype::Int64)
        .at("/tmp/test.table").unwrap();
}
