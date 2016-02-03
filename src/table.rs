use ::table_capnp;
use std::path::{Path, PathBuf};
use std::io;
use std::fmt;
use std::collections::hash_map::HashMap;

#[derive(Copy, Clone)]
/// Basic types suppored by the table backend
pub enum BackendDatatype {
    Byte, Int32, Int64,
    Float, Double,
    FixedLength(i32), VariableLength
}

/// Column types. Not necessarily correspond to the backend types
/// (for example, a boolean column is stored using a Byte)
#[derive(Copy, Clone)]
pub enum ColumnDatatype {
    Byte, Int32, Int64,
    Float, Double,
    FixedLength(i32),
    UTF8, VariableLength,
    Timestamp, TimestampTZ
}

pub struct Column {
    name: String,
    datatype: ColumnDatatype,
    table_ptr: *const Table
}

impl Column {
    pub fn build(name: &str, datatype: ColumnDatatype) -> ColumnBuilder {
        ColumnBuilder {
            name: String::from(name),
            datatype: datatype,
        }
    }
}

#[derive(Clone)]
pub struct ColumnBuilder {
    name: String,
    datatype: ColumnDatatype,
}

impl ColumnBuilder {
    fn create(&self, table: &Table) -> Column {
        Column {
            name: self.name.clone(),
            datatype: self.datatype,
            table_ptr: unsafe { table as *const Table }
        }
    }
}

pub struct Table {
    name: String,
    num_rows: usize,
    columns: Vec<Column>
}

impl Table {
    pub fn build(name: &str) -> TableBuilder {
        TableBuilder {
            name: String::from(name),
            columns: Vec::new()
        }
    }

    pub fn num_columns(&self) -> usize { self.columns.len() }
}

pub struct TableBuilder {
    name: String,
    columns: Vec<ColumnBuilder>
}

enum TableError {
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

type TableResult<T> = Result<T, TableError>;

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

        // Make sure the table names are not duplicated
        let mut nameCount: HashMap<&str, i32> = HashMap::new();
        for ref column in self.columns.iter() {
            let cnt = nameCount.entry(&column.name).or_insert(0);
            *cnt += 1;
            if *cnt > 1 {
                return Err(TableError::InvalidTable(format!("Column '{}' is specified more than once", column.name)));
            }
        }

        let mut table = Table {
            name: self.name.clone(),
            num_rows: 0,
            columns: Vec::new()
        };

        table.columns = self.columns.iter().map(|b| b.create(&table)).collect();
        Ok(table)
    }
}
