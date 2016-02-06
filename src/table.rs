use std::path::{Path, PathBuf};
use std::io;
use std::io::{Write};
use std::fmt;
use std::collections::hash_map::HashMap;
use std::sync::{Arc, RwLock};
use std::fs::File;

// ----------------------------------------------------------------------------
/// Basic types suppored by the table backend
#[derive(Copy, Clone)]
pub enum ColumnDatatype {
    Byte, Int32, Int64,
    Float,
    FixedLength(i32), VariableLength
}

// ----------------------------------------------------------------------------
pub struct Column {
    name: String,
    datatype: ColumnDatatype,
    num_column: usize
}

impl Column {
    pub fn build(name: &str, datatype: ColumnDatatype) -> ColumnBuilder {
        ColumnBuilder {
            name: String::from(name),
            datatype: datatype,
        }
    }

    pub fn datatype(&self) -> &ColumnDatatype { &self.datatype }
    pub fn name(&self) -> &str { &self.name }
    pub fn num_column_in_table(&self) -> usize { self.num_column }
}

// ----------------------------------------------------------------------------
#[derive(Clone)]
pub struct ColumnBuilder {
    name: String,
    datatype: ColumnDatatype,
}

// ----------------------------------------------------------------------------
pub struct Table {
    name: String,
    num_rows: usize,
    columns: Vec<Column>,
    file_path: PathBuf,
    file: File
}

impl Table {
    pub fn build(name: &str) -> TableBuilder {
        TableBuilder {
            name: String::from(name),
            columns: Vec::new()
        }
    }

    pub fn columns(&self) -> &Vec<Column> { &self.columns }
    pub fn column(&self, idx: usize) -> &Column { &self.columns[idx] }
    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|ref c| c.name == name)
    }
    pub fn num_columns(&self) -> usize { self.columns.len() }

    pub fn num_rows(&self) -> usize { self.num_rows }

    pub fn name(&self) -> &str { &self.name }
}

// ----------------------------------------------------------------------------
pub enum TableError {
    FileAlreadyExists,
    InvalidPath(PathBuf),
    InvalidTable(String),
    IoError(io::Error)
}

impl fmt::Debug for TableError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TableError::FileAlreadyExists => write!(f, "File already exists"),
            TableError::InvalidPath(ref p) => write!(f, "Invalid path: {}", p.display()),
            TableError::InvalidTable(ref desc) => write!(f, "Invalid table: {}", desc),
            TableError::IoError(ref e) => e.fmt(f),
        }
    }
}

impl From<io::Error> for TableError {
    fn from(err: io::Error) -> TableError { TableError::IoError(err) }
}

pub type TableResult<T> = Result<T, TableError>;

// ----------------------------------------------------------------------------
pub struct TableBuilder {
    name: String,
    columns: Vec<ColumnBuilder>
}

impl TableBuilder {
    pub fn column(&mut self, name: &str, datatype: ColumnDatatype) -> &mut Self {
        self.columns.push(Column::build(name, datatype));
        self
    }

    /// Creates the table at the specified path
    pub fn at<P: AsRef<Path>>(&self, path_ref: P) -> TableResult<Table> {
        let path = path_ref.as_ref();

        // Check that the file does NOT exist
        if path.is_dir() {
            return Err(TableError::InvalidPath(path.to_owned()));
        } else if path.exists() {
            return Err(TableError::FileAlreadyExists);
        }


        // Check that the parent path exists and it's valid
        match path.parent() {
            None => return Err(TableError::InvalidPath(path.to_owned())),
            Some(ref parent) => {
                if !parent.exists() {
                    return Err(TableError::InvalidPath(path.to_owned()))
                }
            }
        }

        // Make sure the column names are not duplicated
        let mut name_count: HashMap<&str, i32> = HashMap::new();
        for ref column in self.columns.iter() {
            let cnt = name_count.entry(&column.name).or_insert(0);
            *cnt += 1;
            if *cnt > 1 {
                return Err(TableError::InvalidTable(format!("Column '{}' is specified more than once", column.name)));
            }
        }

        // Create the file that will hold this table
        let file = try!(File::create(&path_ref));

        let mut table = Table {
            name: self.name.clone(),
            num_rows: 0,
            columns: Vec::new(),
            file_path: path.to_owned(),
            file: file
        };

        for ref column_builder in &self.columns {
            let column = Column {
                name: column_builder.name.clone(),
                datatype: column_builder.datatype,
                num_column: table.columns.len()
            };

            table.columns.push(column);
        }

        try!(TableFormat::write_header(&mut table));

        Ok(table)
    }
}

// ----------------------------------------------------------------------------
struct TableFormat;
impl TableFormat {
    fn write_header(table: &mut Table) -> io::Result<()> {
        try!(table.file.write("SCF".as_bytes()));
        Ok(())
    }
}

// ----------------------------------------------------------------------------
pub enum ColumnValue {
    Null,
    Byte(u8), Int32(i32), Int64(i64),
    Float(f32),
    FixedLength(Vec<u8>), VariableLength(Vec<u8>)
}

// ----------------------------------------------------------------------------
pub enum InsertError {
    InvalidNumberOfColumns{ got: usize, expected: usize }
}

pub type InsertResult<T> = Result<T, InsertError>;

// ----------------------------------------------------------------------------
pub struct TableInserter {
    table: Arc<RwLock<Table>>
}

impl TableInserter {
    pub fn new(table: Arc<RwLock<Table>>) -> TableInserter {
        TableInserter {
            table: table
        }
    }

    pub fn enqueue_row(&mut self, row: &Vec<ColumnValue>) -> InsertResult<()> {
        let table = self.table.read().unwrap();

        // Validate number of columns
        let expected = table.num_columns();
        let got = row.len();
        if got != expected {
            return Err(InsertError::InvalidNumberOfColumns{
                got: got, expected: expected
            })
        }

        unimplemented!();
    }

    fn flush(&mut self) -> InsertResult<()> {
        let mut table = self.table.write().unwrap();
        unimplemented!();
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, RwLock};

    use std::fs;
    use std::path::PathBuf;

    use ::os;
    use ::table::{Table, ColumnDatatype, TableInserter, ColumnValue};


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

        Table::build("test")
            .column("id", ColumnDatatype::Int32)
            .at(test_path.file_name("test.table")).unwrap();
    }

    #[test]
    fn column_accessors() {
        let test_path = TestPath::new();

        let table = Table::build("test")
            .column("col1", ColumnDatatype::Int32)
            .column("col2", ColumnDatatype::Float)
            .at(test_path.file_name("test.table")).unwrap();

        assert_eq!(table.column(0).name(), "col1");
        assert_eq!(table.column(1).name(), "col2");
        assert!(table.column_by_name("col1").is_some());
        assert!(table.column_by_name("col3").is_none());
        assert_eq!(table.column_by_name("col2").unwrap().num_column_in_table(), 1);
    }

    #[test]
    fn table_generates_right_columns() {
        let test_path = TestPath::new();

        Table::build("test")
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

        let lock: Arc<RwLock<Table>> = Arc::new(RwLock::new(table));
        {
            let mut inserter = TableInserter::new(lock.clone());

            let row = vec!(
                ColumnValue::Null,
                ColumnValue::Byte(2),
                ColumnValue::Int32(300),
                ColumnValue::Int64(400000000i64),
                ColumnValue::Float(3.14159),
                ColumnValue::FixedLength(vec!(1,2,3,4,5)),
                ColumnValue::VariableLength("Hello world".to_string().into()),
            );
            inserter.enqueue_row(&row);
        }

        // Restore the table handler
        table = Arc::try_unwrap(lock).ok().expect("Pending references to table mutex!")
            .into_inner().unwrap();               // ... which gives us back the table.

        assert_eq!(table.num_rows(), 1);

    }
}
