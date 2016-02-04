#[cfg(test)]
use std::ptr;
use std::ffi::{CStr, CString};
use std::fs;
use std::os::raw::{c_char, c_void};
use std::path::PathBuf;

use ::table::{ColumnDatatype, Table, ColumnValue, TableBuilder};
use ::os;

struct TestPath {
    path: PathBuf
}

impl TestPath {
    fn new() -> TestPath {
        let path = os::tempname("table");
        fs::create_dir(&path);

        TestPath {
            path: path
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
        fs::remove_dir_all(&self.path).ok();
    }
}

#[test]
fn table_can_be_built() {
    let test_path = TestPath::new();

    let table = Table::build("test")
        .column("id", ColumnDatatype::Int32)
        .at(test_path.file_name("test.table")).unwrap();
}

#[test]
fn table_generates_right_columns() {
    let test_path = TestPath::new();

    let table = Table::build("test")
        .column("col1", ColumnDatatype::Int32)
        .column("col2", ColumnDatatype::Int32)
        .at(test_path.file_name("test.table")).unwrap();
}

#[test]
fn table_builder_in_invalid_path() {
    let builder = Table::build("test") ;
    assert!(builder.at("/invalid/path/test.table").is_err());
    assert!(builder.at("/tmp").is_err());
    assert!(builder.at("/").is_err());
    assert!(builder.at("").is_err());
}

#[test]
#[should_panic(expected="more than once")]
fn table_with_duplicated_columns() {
    let table = Table::build("test")
        .column("id", ColumnDatatype::Int32)
        .column("id", ColumnDatatype::Int64)
        .at("/tmp/test.table").unwrap();
}

#[test]
fn a_single_row_can_be_inserted() {
    let test_path = TestPath::new();

    let mut table = Table::build("test")
        .column("nullcol", ColumnDatatype::Byte)
        .column("bytecol", ColumnDatatype::Byte)
        .column("int32col", ColumnDatatype::Int32)
        .column("int64col", ColumnDatatype::Int64)
        .column("floatcol", ColumnDatatype::Float)
        .column("fixedlengthcol", ColumnDatatype::FixedLength(5))
        .column("variablelengthcol", ColumnDatatype::VariableLength)
        .at(test_path.file_name("test.table")).unwrap();

    {
        let mut inserter = table.create_inserter();
        let row = vec!(
            ColumnValue::Null,
            ColumnValue::Byte(2),
            ColumnValue::Int32(300),
            ColumnValue::Int64(400000000i64),
            ColumnValue::Float(3.14159),
            ColumnValue::FixedLength(vec!(1,2,3,4,5)),
            ColumnValue::VariableLength("Hello world".to_string().into()),
        );
        inserter.insert_row(&row);
    }

    assert_eq!(table.num_rows(), 1);
}
