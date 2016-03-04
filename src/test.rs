use std::path::{Path, PathBuf};
use std::fs;

use ::storage::{Storage, StorageBuilder, ColumnDatatype, ColumnValue};

// ----------------------------------------------------------------------------
use libc::{c_char, c_void, free};
use std::ffi::{CString, CStr};

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

// ----------------------------------------------------------------------------
pub struct TestPath {
    path: PathBuf,
    delete: bool
}

impl TestPath {
    pub fn new() -> TestPath {
        let path = tempname("storage");
        fs::create_dir(&path).unwrap();

        TestPath {
            path: path,
            delete: true
        }
    }

    pub fn file_name(&self, name: &str) -> PathBuf {
        let mut tmp = self.path.clone();
        tmp.push(name);
        tmp
    }
}

impl Drop for TestPath {
    fn drop(&mut self) {
        if self.delete {
            fs::remove_dir_all(&self.path).ok();
        }
    }
}

// ----------------------------------------------------------------------------
/// A storage that is commonly used for tests
pub struct TestStorage;

impl TestStorage {
    pub fn new(path: &Path) -> Storage {
        StorageBuilder::new()
            .column("nullcol", ColumnDatatype::Byte)
            .column("bytecol", ColumnDatatype::Byte)
            .column("int32col", ColumnDatatype::Int32)
            .column("int64col", ColumnDatatype::Int64)
            .column("floatcol", ColumnDatatype::Float)
            .column("fixedlengthcol", ColumnDatatype::FixedLength(5))
            .column("variablelengthcol", ColumnDatatype::VariableLength)
            .at(path).unwrap()
    }
}

// ----------------------------------------------------------------------------
#[test]
fn column_accessors() {
    let test_path = TestPath::new();

    let storage = StorageBuilder::new()
        .column("col1", ColumnDatatype::Int32)
        .column("col2", ColumnDatatype::Float)
        .at(test_path.file_name("test.storage")).unwrap();

    assert_eq!(storage.column(0).name(), "col1");
    assert_eq!(storage.column(1).name(), "col2");
    assert!(storage.column_by_name("col1").is_some());
    assert!(storage.column_by_name("col3").is_none());
    assert_eq!(storage.column_by_name("col2").unwrap().num_column_in_storage(), 1);
}

// ----------------------------------------------------------------------------
#[test]
fn storage_generates_right_columns() {
    StorageBuilder::new()
        .column("col1", ColumnDatatype::Int32)
        .column("col2", ColumnDatatype::Int32)
        .in_memory()
        .unwrap();
}

// ----------------------------------------------------------------------------
#[test]
fn storage_builder_in_valid_path() {
    let test_path = TestPath::new();
    let test_file = test_path.file_name("test.storage");
    {
        StorageBuilder::new()
            .column("id", ColumnDatatype::Int32)
            .at(&test_file)
            .unwrap();
    }

    // Check that the file exists
    assert!(test_file.metadata().is_ok());
}

// ----------------------------------------------------------------------------
#[test]
fn storage_builder_in_invalid_path() {
    let builder = StorageBuilder::new();
    assert!(builder.at("/invalid/path/test.storage").is_err());
    assert!(builder.at("/tmp").is_err());
    assert!(builder.at("/").is_err());
    assert!(builder.at("").is_err());
}

// ----------------------------------------------------------------------------
#[test]
#[should_panic(expected="more than once")]
fn storage_with_duplicated_columns() {
    StorageBuilder::new()
        .column("id", ColumnDatatype::Int32)
        .column("id", ColumnDatatype::Int64)
        .in_memory()
        .unwrap();
}

// ----------------------------------------------------------------------------
#[test]
fn a_single_row_can_be_inserted() {
    let test_path = TestPath::new();
    let test_file = test_path.file_name("test.storage");

    let mut storage = TestStorage::new(test_file.as_path());
    let mut insertion_manager = storage.begin_inserting();
    {
        let mut inserter = insertion_manager.create_inserter();

        let row = vec!(
            ColumnValue::Null,
            ColumnValue::Byte(2),
            ColumnValue::Int32(300),
            ColumnValue::Int64(400000000i64),
            ColumnValue::Float(3.14159),
            ColumnValue::FixedLength(vec!(1,2,3,4,5)),
            ColumnValue::VariableLength("Hello world".to_string().into()),
        );

        let result = inserter.enqueue_row(&row);
        assert!(result.is_ok());
    }

    storage = insertion_manager.finish_inserting().unwrap();
    assert_eq!(storage.num_rows(), 1);

}

// ----------------------------------------------------------------------------
#[test]
fn invalid_values_cannot_be_inserted() {
    let test_path = TestPath::new();

    let storage = TestStorage::new(test_path.file_name("test.storage").as_path());
    let mut insertion_manager = storage.begin_inserting();
    {
        let mut inserter = insertion_manager.create_inserter();

        let row = vec!(
            ColumnValue::Null,
            ColumnValue::Byte(2),
            // This should be an Int32
            ColumnValue::Int64(300),
            ColumnValue::Int64(400000000i64),
            ColumnValue::Float(3.14159),
            ColumnValue::FixedLength(vec!(1,2,3,4,5)),
            ColumnValue::VariableLength("Hello world".to_string().into()),
        );

        let result = inserter.enqueue_row(&row);
        assert!(result.is_err());
    }
}
