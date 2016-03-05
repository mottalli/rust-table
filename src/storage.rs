use std::path::{Path, PathBuf};
use std::io;
use std::io::{Read, Write, Seek, SeekFrom, Cursor};
use std::fmt;
use std::collections::hash_map::HashMap;
use std::fs::File;
use std::iter::Iterator;
use std::str;
use std::{i8, i32, i64, f32};

use ::proto_structs;
use ::storage_inserter::InsertionManager;

// ----------------------------------------------------------------------------
/// Basic types suppored by the storage backend
#[derive(Debug, Copy, Clone)]
pub enum ColumnDatatype {
    Byte, Int32, Int64,
    Float,
    FixedLength(i32), VariableLength
}

impl fmt::Display for ColumnDatatype {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ColumnDatatype::Byte => write!(f, "Byte"),
            ColumnDatatype::Int32 => write!(f, "Int32"),
            ColumnDatatype::Int64 => write!(f, "Int64"),
            ColumnDatatype::Float => write!(f, "Float"),
            ColumnDatatype::FixedLength(s) => write!(f, "FixedLength({})", s),
            ColumnDatatype::VariableLength => write!(f, "VariableLength"),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct DatatypeInfo {
    pub is_numeric: bool,
    pub is_fixed_size: bool,
    pub value_size: Option<usize>
}

impl DatatypeInfo {
    fn new(datatype: &ColumnDatatype) -> DatatypeInfo {
        match *datatype {
            ColumnDatatype::Byte => DatatypeInfo { is_numeric: true, is_fixed_size: true, value_size: Some(1) },
            ColumnDatatype::Int32 => DatatypeInfo { is_numeric: true, is_fixed_size: true, value_size: Some(4) },
            ColumnDatatype::Int64 => DatatypeInfo { is_numeric: true, is_fixed_size: true, value_size: Some(8) },
            ColumnDatatype::Float => DatatypeInfo { is_numeric: true, is_fixed_size: true, value_size: Some(4) },
            ColumnDatatype::FixedLength(s) => DatatypeInfo { is_numeric: false, is_fixed_size: true, value_size: Some(s as usize) },
            ColumnDatatype::VariableLength => DatatypeInfo { is_numeric: false, is_fixed_size: false, value_size: None },
        }
    }
}

// ----------------------------------------------------------------------------
pub struct Column {
    pub name: String,
    pub datatype: ColumnDatatype,
    pub datatype_info: DatatypeInfo,
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
    pub fn num_column_in_storage(&self) -> usize { self.num_column }
}

// ----------------------------------------------------------------------------
#[derive(Clone)]
pub struct ColumnBuilder {
    name: String,
    datatype: ColumnDatatype,
}

// ----------------------------------------------------------------------------
pub trait StorageBackend : Read + Write + Seek {}
impl StorageBackend for File {}
impl StorageBackend for Cursor<Vec<u8>> {}

// ----------------------------------------------------------------------------
pub struct Storage
{
    pub num_rows: usize,
    pub columns: Vec<Column>,
    pub backend: Box<StorageBackend>,
    stripes: Vec<proto_structs::Stripe>
}

impl Storage
{
    fn init(backend: Box<StorageBackend>, builder: &StorageBuilder) -> StorageResult<Storage> {
        // Make sure the column names are not duplicated
        let mut name_count: HashMap<&str, i32> = HashMap::new();
        for ref column in builder.columns.iter() {
            let cnt = name_count.entry(&column.name).or_insert(0);
            *cnt += 1;
            if *cnt > 1 {
                return Err(StorageError::InvalidFormat(format!("Column '{}' is specified more than once", column.name)));
            }
        }

        // Create the columns
        let columns: Vec<Column> = builder.columns.iter().enumerate().map(|(i,b)| {
            Column {
                name: b.name.clone(),
                datatype: b.datatype,
                datatype_info: DatatypeInfo::new(&b.datatype),
                num_column: i
            }
        }).collect();

        let mut storage = Storage {
            num_rows: 0,
            columns: columns,
            backend: backend,
            stripes: Vec::new()
        };

        try!(storage.write_header());

        Ok(storage)
    }

    pub fn write_header(&mut self) -> StorageResult<()> {
        try!(self.backend.write(Self::signature()));
        Ok(())
    }

    pub fn write_footer(&mut self) -> StorageResult<()> {
        try!(self.backend.seek(SeekFrom::End(0)));
        try!(self.backend.write(Self::signature()));
        Ok(())
    }

    fn signature() -> &'static [u8] {
        // "Snel Columnar Storage"
        "SCS".as_bytes()
    }

    pub fn columns(&self) -> &Vec<Column> { &self.columns }
    pub fn column(&self, idx: usize) -> &Column { &self.columns[idx] }
    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|ref c| c.name == name)
    }
    pub fn num_columns(&self) -> usize { self.columns.len() }
    pub fn num_rows(&self) -> usize { self.num_rows }

    pub fn begin_inserting(self) -> InsertionManager {
        InsertionManager::new(self)
    }

    //TODO: Make this function non-public
    pub fn append_stripe(&mut self, stripe: &proto_structs::Stripe) {
        self.stripes.push((*stripe).clone());
        self.num_rows += stripe.num_rows;
    }
}

// ----------------------------------------------------------------------------
#[derive(Debug)]
pub enum StorageError {
    FileAlreadyExists,
    InvalidPath(PathBuf),
    InvalidFormat(String),
    IoError(io::Error),
    InvalidNumberOfColumns(usize, usize),
    TypeError,
    InvalidLength(usize, usize)
}

/*impl fmt::Debug for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            StorageError::FileAlreadyExists => write!(f, "File already exists"),
            StorageError::InvalidPath(ref p) => write!(f, "Invalid path: {}", p.display()),
            StorageError::InvalidFormat(ref desc) => write!(f, "Invalid storage format: {}", desc),
            StorageError::IoError(ref e) => e.fmt(f),
        }
    }
}*/

impl From<io::Error> for StorageError {
    fn from(err: io::Error) -> StorageError { StorageError::IoError(err) }
}

pub type StorageResult<T> = Result<T, StorageError>;

// ----------------------------------------------------------------------------
pub struct StorageBuilder {
    columns: Vec<ColumnBuilder>
}

impl StorageBuilder {
    pub fn new() -> StorageBuilder {
        StorageBuilder { columns: Vec::new() }
    }

    pub fn column(&mut self, name: &str, datatype: ColumnDatatype) -> &mut Self {
        self.columns.push(Column::build(name, datatype));
        self
    }

    /// Creates the storage at the specified path
    pub fn at<P: AsRef<Path>>(&self, path_ref: P) -> StorageResult<Storage> {
        let path = path_ref.as_ref();

        // Check that the file does NOT exist
        if path.is_dir() {
            return Err(StorageError::InvalidPath(path.to_owned()));
        } else if path.exists() {
            return Err(StorageError::FileAlreadyExists);
        }

        // Check that the parent path exists and it's valid
        match path.parent() {
            None => return Err(StorageError::InvalidPath(path.to_owned())),
            Some(ref parent) => {
                if !parent.exists() {
                    return Err(StorageError::InvalidPath(path.to_owned()))
                }
            }
        }

        // Create the file that will hold this storage
        let file = try!(File::create(&path_ref));

        Storage::init(Box::new(file), self)
    }

    pub fn in_memory(&self) -> StorageResult<Storage> {
        let mem_backend = Cursor::new(Vec::<u8>::new());
        Storage::init(Box::new(mem_backend), self)
    }
}


// ----------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Null,
    Byte(i8), Int32(i32), Int64(i64),
    Float(f32),
    FixedLength(Vec<u8>), VariableLength(Vec<u8>)
}

impl fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn write_bytes<'a>(f: &mut fmt::Formatter, iter: &mut Iterator<Item=&'a u8>) -> fmt::Result {
            while let Some(b) = iter.next() {
                try!(write!(f, "{:X} ", b));
            }

            Ok(())
        }

        match *self {
            ColumnValue::Null => { write!(f, "(NULL)") },
            ColumnValue::Byte(v) => { write!(f, "Byte({})", v) },
            ColumnValue::Int32(v) => { write!(f, "Int32({})", v) },
            ColumnValue::Int64(v) => { write!(f, "Int64({})", v) },
            ColumnValue::Float(v) => { write!(f, "Float({})", v) },
            ColumnValue::FixedLength(ref v) => {
                write!(f, "FixedLength(")
                    .and(write_bytes(f, &mut v.iter().take(5)))
                    .and(if v.len() > 5 {write!(f, "...")} else {Ok(())})
                    .and(write!(f, ")"))
            },
            ColumnValue::VariableLength(ref v) => {
                // Try to convert the value to a string. If not possible, display the raw bytes.
                write!(f, "VariableLength(")
                    .and(match str::from_utf8(v) {
                        Ok(s) => write!(f, "\"{}\"", s),
                        Err(_) => write_bytes(f, &mut v.iter().take(5))
                            .and(if v.len() > 5 {write!(f, "...")} else {Ok(())})
                    })
                    .and(write!(f, ")"))
            }
        }
    }
}

// ----------------------------------------------------------------------------
pub trait NumericValue: Sized {
    /// Extract exactly a value of this type from the given value.
    /// It should not handle NULL cases, this is done by extract_value_or_null
    fn extract_value_exact(value: &ColumnValue) -> Option<Self>;
    /// The column datatype used for storing a value of this type
    fn datatype() -> ColumnDatatype;
    /// The null value associated to this type
    fn null_value() -> Self;

    /// Extract a value of this type or the NULL value. Returns an error
    /// if the value is not NULL or it is not of this type.
    fn extract_value_or_null(value: &ColumnValue) -> StorageResult<Option<Self>> {
        if let ColumnValue::Null = *value {
            // Return "No value"
            return Ok(None)
        }

        match Self::extract_value_exact(value) {
            Some(v) => Ok(Some(v)),
            None => // The value could not be extracted
                Err(StorageError::TypeError)
        }
    }
}

impl NumericValue for i8 {
    fn extract_value_exact(value: &ColumnValue) -> Option<Self> {
        match *value {
            ColumnValue::Byte(v) => Some(v),
            _ => None
        }
    }

    fn datatype() -> ColumnDatatype { ColumnDatatype::Byte }
    fn null_value() -> Self { i8::MIN }
}

impl NumericValue for i32 {
    fn extract_value_exact(value: &ColumnValue) -> Option<Self> {
        match *value {
            ColumnValue::Int32(v) => Some(v),
            _ => None
        }
    }

    fn datatype() -> ColumnDatatype { ColumnDatatype::Int32 }
    fn null_value() -> Self { i32::MIN }
}

impl NumericValue for i64 {
    fn extract_value_exact(value: &ColumnValue) -> Option<Self> {
        match *value {
            ColumnValue::Int64(v) => Some(v),
            _ => None
        }
    }

    fn datatype() -> ColumnDatatype { ColumnDatatype::Int64 }
    fn null_value() -> Self { i64::MIN }
}

impl NumericValue for f32 {
    fn extract_value_exact(value: &ColumnValue) -> Option<Self> {
        match *value {
            ColumnValue::Float(v) => Some(v),
            _ => None
        }
    }

    fn datatype() -> ColumnDatatype { ColumnDatatype::Float }
    fn null_value() -> Self { f32::NEG_INFINITY }
}

// ----------------------------------------------------------------------------
#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};

    use ::test::{TestPath};
    use ::storage::{Storage, StorageBuilder, ColumnDatatype};

    #[test]
    fn storage_can_be_initialized() {
        let test_path = TestPath::new();
        let filename = test_path.file_name("test.storage");

        StorageBuilder::new()
            .column("id", ColumnDatatype::Int32)
            .at(&filename)
            .unwrap();

        // Check that the file has the right header and footer
        let mut file = File::open(&filename).unwrap();
        let expected_signature = Storage::signature();
        let mut buf = Vec::<u8>::new();
        buf.resize(expected_signature.len(), 0);

        file.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..], expected_signature);

        // Check the footer
        file.seek(SeekFrom::End(-(expected_signature.len() as i64))).unwrap();
        file.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..], expected_signature);
    }


}
