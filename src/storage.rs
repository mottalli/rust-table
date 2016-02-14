use std::path::{Path, PathBuf};
use std::io;
use std::io::{Read, Write, Seek, Cursor};
use std::fmt;
use std::collections::hash_map::HashMap;
use std::sync::{Arc, RwLock};
use std::fs::File;
use std::iter::Iterator;
use std::str;
use std::{i8, i32, i64, f32};
use std::mem;
use std::slice;

use capnp::message::{Builder as ProtoBuilder};

use ::proto_structs;
use ::proto_structs::ProtocolBuildable;
use ::encoding::Encoding;
use ::compression::Compression;

// ----------------------------------------------------------------------------
/// Helper function
fn get_slice_bytes<'a, T>(s: &'a [T]) -> &'a [u8]
    where T: Sized
{
    let ptr = s.as_ptr() as *const u8;
    let size = mem::size_of::<T>() * s.len();
    unsafe { slice::from_raw_parts(ptr, size) }
}

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
struct DatatypeInfo {
    is_numeric: bool,
    is_fixed_size: bool,
    value_size: usize
}

impl DatatypeInfo {
    fn new(datatype: &ColumnDatatype) -> DatatypeInfo {
        match *datatype {
            ColumnDatatype::Byte => DatatypeInfo { is_numeric: true, is_fixed_size: true, value_size: 1 },
            ColumnDatatype::Int32 => DatatypeInfo { is_numeric: true, is_fixed_size: true, value_size: 4 },
            ColumnDatatype::Int64 => DatatypeInfo { is_numeric: true, is_fixed_size: true, value_size: 8 },
            ColumnDatatype::Float => DatatypeInfo { is_numeric: true, is_fixed_size: true, value_size: 4 },
            ColumnDatatype::FixedLength(s) => DatatypeInfo { is_numeric: false, is_fixed_size: true, value_size: s as usize },
            ColumnDatatype::VariableLength => DatatypeInfo { is_numeric: false, is_fixed_size: false, value_size: 0 },
        }
    }
}

// ----------------------------------------------------------------------------
pub struct Column {
    name: String,
    datatype: ColumnDatatype,
    datatype_info: DatatypeInfo,
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
    num_rows: usize,
    columns: Vec<Column>,
    backend: Box<StorageBackend>,
    stripes: Vec<proto_structs::Stripe>
}

impl Storage
{
    fn init<B: StorageBackend + 'static>(backend: B, builder: &StorageBuilder) -> StorageResult<Storage> {
        // Make sure the column names are not duplicated
        let mut name_count: HashMap<&str, i32> = HashMap::new();
        for ref column in builder.columns.iter() {
            let cnt = name_count.entry(&column.name).or_insert(0);
            *cnt += 1;
            if *cnt > 1 {
                return Err(StorageError::InvalidFormat(format!("Column '{}' is specified more than once", column.name)));
            }
        }

        let mut storage = Storage {
            num_rows: 0,
            columns: Vec::new(),
            backend: Box::new(backend),
            stripes: Vec::new()
        };

        // Create the columns
        for ref column_builder in &builder.columns {
            let column = Column {
                name: column_builder.name.clone(),
                datatype: column_builder.datatype,
                datatype_info: DatatypeInfo::new(&column_builder.datatype),
                num_column: storage.columns.len()
            };

            storage.columns.push(column);
        }

        Ok(storage)
    }

    pub fn columns(&self) -> &Vec<Column> { &self.columns }
    pub fn column(&self, idx: usize) -> &Column { &self.columns[idx] }
    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|ref c| c.name == name)
    }
    pub fn num_columns(&self) -> usize { self.columns.len() }

    pub fn num_rows(&self) -> usize { self.num_rows }

    /// A hint for how many rows should fit in a storage stripe
    fn num_rows_in_stripe_hint(&self) -> usize {
        let disk_block_size: usize = 4096;
        // How many blocks in a stripe
        let blocks_in_stripe: usize = 64;

        // Find, for all the numeric columns, the one with the biggest size.
        let max_size = self.columns.iter()
            .filter(|c| c.datatype_info.is_numeric)
            .map(|c| c.datatype_info.value_size)
            .max().unwrap_or(1);    // If there are no numeric colums, assume size 1

        (blocks_in_stripe*disk_block_size) / max_size
    }

    fn append_stripe(&mut self, num_rows: usize, stripe: &Vec<EncodedChunk>) -> StorageResult<()> {
        // No columns to insert? Weird...
        if stripe.len() == 0 { return Ok(()); }

        // Compress the chunks
        //TODO
        let compressed_chunks: Vec<CompressedChunk> = stripe.iter()
            .map(|&EncodedChunk(encoding, chunk)| CompressedChunk(Compression::None, encoding, chunk))
            .collect();

        // Calculate the size of the stripe. It is the sum of the sizes of the compressed chunks.
        // We cannot do this because of issue #27739 :(
        //let stripe_size: usize = compressed_chunks.iter().map(|&CompressedChunk(_, _, c)| c.len()).sum();
        let stripe_size: usize = {
            let mut stripe_size = 0;
            for &CompressedChunk(_, _, c) in compressed_chunks.iter() {
                stripe_size += c.len();
            }
            stripe_size
        };

        let stripe_header_absolute_offset = self.current_offset();

        // Build the stripe header
        let mut stripe_header = proto_structs::StripeHeader {
            num_rows: num_rows,
            column_chunks: Vec::new(),
            stripe_size: stripe_size
        };

        let mut relative_column_begin: usize = 0;
        for (&CompressedChunk(compression, encoding, compressed_chunk), &EncodedChunk(_, encoded_chunk)) in compressed_chunks.iter().zip(stripe.iter()) {
            stripe_header.column_chunks.push(proto_structs::ColumnChunkHeader {
                relative_offset: relative_column_begin,
                compressed_size: compressed_chunk.len(),
                uncompressed_size: encoded_chunk.len(),
                encoding: encoding,
                compression: compression,
            });

            relative_column_begin += compressed_chunk.len();
        }

        // Write the stripe header
        {
            let mut builder = ProtoBuilder::new_default();
            {
                let mut header_builder = builder.init_root::<<proto_structs::StripeHeader as proto_structs::ProtocolBuildable>::Builder>();
                stripe_header.build_message(&mut header_builder);
            }
            try!(::capnp::serialize::write_message(&mut self.backend, &builder));
        }

        // Now write all the compressed columns
        for &CompressedChunk(_, _, chunk) in compressed_chunks.iter() {
            self.backend.write(chunk);
        }

        self.stripes.push(proto_structs::Stripe {
            absolute_offset: stripe_header_absolute_offset,
            num_rows: num_rows
        });

        self.num_rows += num_rows;

        Ok(())
    }

    fn current_offset(&mut self) -> usize {
        self.backend.seek(io::SeekFrom::Current(0)).unwrap() as usize
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
    fn new() -> StorageBuilder {
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

        Storage::init(file, self)
    }

    pub fn in_memory(&self) -> StorageResult<Storage> {
        let mem_backend = Cursor::new(Vec::<u8>::new());
        Storage::init(mem_backend, self)
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
                write!(f, "{:X} ", b);
            }

            Ok(())
        }

        match *self {
            ColumnValue::Null => { write!(f, "(NULL)"); },
            ColumnValue::Byte(v) => { write!(f, "Byte({})", v); },
            ColumnValue::Int32(v) => { write!(f, "Int32({})", v); },
            ColumnValue::Int64(v) => { write!(f, "Int64({})", v); },
            ColumnValue::Float(v) => { write!(f, "Float({})", v); },
            ColumnValue::FixedLength(ref v) => {
                write!(f, "FixedLength(");
                try!(write_bytes(f, &mut v.iter().take(5)));
                if v.len() > 5 {
                    write!(f, "...");
                }
                write!(f, ")");
            },
            ColumnValue::VariableLength(ref v) => {
                write!(f, "VariableLength(");
                // Try to convert the value to a string. If not possible,
                // display the raw bytes.
                match str::from_utf8(v) {
                    Ok(s) => { write!(f, "\"{}\"", s); },
                    Err(_) => {
                        try!(write_bytes(f, &mut v.iter().take(5)));
                        if v.len() > 5 {
                            write!(f, "...");
                        }
                    }
                };
                write!(f, ")");
            }
        };
        Ok(())
    }
}

// ----------------------------------------------------------------------------
trait NumericValue: Sized {
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
pub struct EncodedChunk<'a>(pub Encoding, pub &'a [u8]);
pub struct CompressedChunk<'a>(pub Compression, pub Encoding, pub &'a [u8]);

trait ChunkGenerator {
    fn validate_value(&self, value: &ColumnValue) -> StorageResult<()>;
    fn get_encoded_chunk<'a>(&'a mut self) -> EncodedChunk<'a>;
    fn reset(&mut self);

    /// Precondition: self.validate_value(value).is_ok()
    fn append_values<'a>(&mut self, values: &mut Iterator<Item=&'a ColumnValue>);
}

struct NumericChunkGenerator<N> {
    values: Vec<N>
}

impl<N> NumericChunkGenerator<N> {
    fn new(num_values: usize) -> NumericChunkGenerator<N> {
        NumericChunkGenerator {
            values: Vec::with_capacity(num_values)
        }
    }
}

impl<N> ChunkGenerator for NumericChunkGenerator<N>
    where N: NumericValue
{
    fn validate_value(&self, value: &ColumnValue) -> StorageResult<()> {
        try!(N::extract_value_or_null(value));
        Ok(())
    }

    fn append_values<'a>(&mut self, values: &mut Iterator<Item=&'a ColumnValue>) {
        while let Some(ref value) = values.next() {
            let v = match N::extract_value_or_null(value).unwrap() {
                Some(v) => v,
                None => N::null_value()
            };
            self.values.push(v);
        }
    }

    fn get_encoded_chunk<'a>(&'a mut self) -> EncodedChunk<'a> {
        let result = get_slice_bytes(&self.values);
        EncodedChunk(Encoding::Raw, result)
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

// ----------------------------------------------------------------------------
struct FixedLengthChunkGenerator {
    value_size: usize,
    nulls: Vec<bool>,
    values: Vec<u8>,
    encoded_chunk_buffer: Vec<u8>
}

impl FixedLengthChunkGenerator {
    fn new(value_size: i32, num_values: usize) -> FixedLengthChunkGenerator {
        FixedLengthChunkGenerator {
            value_size: value_size as usize,
            nulls: Vec::with_capacity(num_values),
            values: Vec::with_capacity(num_values*value_size as usize),
            encoded_chunk_buffer: Vec::new()
        }
    }
}

impl ChunkGenerator for FixedLengthChunkGenerator {
    fn validate_value(&self, value: &ColumnValue) -> StorageResult<()> {
        match *value {
            ColumnValue::Null => Ok(()),
            ColumnValue::FixedLength(ref v) => {
                if v.len() == self.value_size {
                    Ok(())
                } else {
                    Err(StorageError::InvalidLength(v.len(), self.value_size))
                }
            },
            _ => Err(StorageError::TypeError)
        }
    }

    fn append_values<'a>(&mut self, values: &mut Iterator<Item=&'a ColumnValue>) {
        while let Some(ref value) = values.next() {
            match **value {
                ColumnValue::Null => self.nulls.push(true),
                ColumnValue::FixedLength(ref v) => {
                    self.nulls.push(false);
                    self.values.write(&v[..]).unwrap();
                },
                // Should never get to this point
                _ => panic!("Internal error: Received an invalid value size")
            }
        }
    }

    fn get_encoded_chunk<'a>(&'a mut self) -> EncodedChunk<'a> {
        let nulls: Vec<u8> = self.nulls.iter().map(|n| if *n { 1 } else { 0 }).collect();

        self.encoded_chunk_buffer.clear();
        self.encoded_chunk_buffer.write(&nulls);
        self.encoded_chunk_buffer.write(&self.values);

        EncodedChunk(Encoding::Raw, &self.encoded_chunk_buffer)
    }

    fn reset(&mut self) {
        self.nulls.clear();
        self.values.clear();
    }
}

// ----------------------------------------------------------------------------
struct VariableLengthChunkGenerator {
    sizes: Vec<i32>,
    values: Vec<u8>,
    encoded_chunk_buffer: Vec<u8>
}

impl VariableLengthChunkGenerator {
    fn new(num_values: usize) -> VariableLengthChunkGenerator {
        VariableLengthChunkGenerator {
            sizes: Vec::with_capacity(num_values),
            values: Vec::new(),
            encoded_chunk_buffer: Vec::new()
        }
    }
}

impl ChunkGenerator for VariableLengthChunkGenerator {
    fn validate_value(&self, value: &ColumnValue) -> StorageResult<()> {
        match *value {
            ColumnValue::Null => Ok(()),
            ColumnValue::VariableLength(_) => Ok(()),
            _ => Err(StorageError::TypeError)
        }
    }

    fn append_values<'a>(&mut self, values: &mut Iterator<Item=&'a ColumnValue>) {
        while let Some(ref value) = values.next() {
            match **value {
                ColumnValue::Null => self.sizes.push(-1),
                ColumnValue::VariableLength(ref v) => {
                    self.sizes.push(v.len() as i32);
                    self.values.write(v).unwrap();
                },
                // Should never get to this point
                _ => panic!("Internal error: Received an invalid value")
            }
        }
    }

    fn get_encoded_chunk<'a>(&'a mut self) -> EncodedChunk<'a> {
        self.encoded_chunk_buffer.clear();
        self.encoded_chunk_buffer.write(get_slice_bytes(&self.sizes));
        self.encoded_chunk_buffer.write(&self.values);

        EncodedChunk(Encoding::Raw, &self.encoded_chunk_buffer)
    }

    fn reset(&mut self) {
        self.sizes.clear();
        self.values.clear();
    }
}
// ----------------------------------------------------------------------------
pub struct StorageInserter
{
    storage: Arc<RwLock<Storage>>,
    enqueued_rows: Vec<Vec<ColumnValue>>,
    chunk_generators: Vec<Box<ChunkGenerator>>,
    max_rows_in_stripe: usize
}

impl StorageInserter
{
    fn get_chunk_generator_for_datatype(datatype: &ColumnDatatype, size: usize) -> Box<ChunkGenerator> {
        match *datatype {
            ColumnDatatype::Byte => Box::new(NumericChunkGenerator::<i8>::new(size)),
            ColumnDatatype::Int32 => Box::new(NumericChunkGenerator::<i32>::new(size)),
            ColumnDatatype::Int64 => Box::new(NumericChunkGenerator::<i64>::new(size)),
            ColumnDatatype::Float => Box::new(NumericChunkGenerator::<f32>::new(size)),
            ColumnDatatype::FixedLength(length) => Box::new(FixedLengthChunkGenerator::new(length, size)),
            ColumnDatatype::VariableLength => Box::new(VariableLengthChunkGenerator::new(size)),
        }
    }

    pub fn new(storage: Arc<RwLock<Storage>>) -> StorageInserter {
        let (max_rows_in_stripe, chunk_generators) = {
            // Acquire read lock
            let storage = storage.read().unwrap();

            let max_rows_in_stripe = storage.num_rows_in_stripe_hint();
            let chunk_generators: Vec<Box<ChunkGenerator>> = storage.columns().iter()
                .map(|c| Self::get_chunk_generator_for_datatype(&c.datatype, max_rows_in_stripe))
                .collect();

            (max_rows_in_stripe, chunk_generators)
        };

        StorageInserter {
            storage: storage,
            enqueued_rows: Vec::new(),
            chunk_generators: chunk_generators,
            max_rows_in_stripe: max_rows_in_stripe,
        }
    }

    pub fn enqueue_row(&mut self, row: &Vec<ColumnValue>) -> StorageResult<()> {
        // Validate number of columns
        let expected = self.storage.read().unwrap().num_columns();
        let got = row.len();
        if got != expected {
            return Err(StorageError::InvalidNumberOfColumns(got, expected))
        }

        // Make sure that all the values have the right types
        for (chunk_generator, value) in self.chunk_generators.iter().zip(row.iter()) {
            try!(chunk_generator.validate_value(value));
        }

        self.enqueued_rows.push(row.clone());

        if self.enqueued_rows.len() == self.max_rows_in_stripe {
            self.flush()
        } else {
            Ok(())
        }
    }

    fn flush(&mut self) -> StorageResult<()> {
        if self.enqueued_rows.len() == 0 {
            return Ok(())
        }

        // Send the values to the appropriate chunk generator
        for (i, chunk_generator) in self.chunk_generators.iter_mut().enumerate() {
            let mut values_iter = self.enqueued_rows.iter().map(|ref r| &r[i]);
            chunk_generator.append_values(&mut values_iter);
        }

        // Write the chunks!
        {
            // Acquire write lock for storage
            let mut storage = self.storage.write().unwrap();

            {
                let encoded_stripe: Vec<EncodedChunk> = self.chunk_generators.iter_mut()
                    .map(|gen| gen.get_encoded_chunk())
                    .collect();

                try!(storage.append_stripe(self.enqueued_rows.len(), &encoded_stripe));
            }

            for chunk_generator in self.chunk_generators.iter_mut() {
                chunk_generator.reset();
            }
        }

        self.enqueued_rows.clear();
        Ok(())
    }
}

impl Drop for StorageInserter
{
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

// ----------------------------------------------------------------------------
#[cfg(test)]
mod test {
    use std::sync::{Arc, RwLock};

    use std::fs;
    use std::fs::{File};
    use std::path::{Path, PathBuf};
    use std::io::Cursor;

    use ::os;
    use ::storage::{Storage, StorageBuilder, ColumnDatatype, StorageInserter, ColumnValue};


    struct TestPath {
        path: PathBuf,
        delete: bool
    }

    impl TestPath {
        fn new() -> TestPath {
            let path = os::tempname("storage");
            fs::create_dir(&path).unwrap();

            TestPath {
                path: path,
                delete: true
            }
        }

        fn file_name(&self, name: &str) -> PathBuf {
            let mut tmp = self.path.clone();
            tmp.push(name);
            tmp
        }

        fn dont_delete(&mut self) {
            self.delete = false;
        }
    }

    /// A storage that is commonly used for tests
    struct TestStorage;

    impl TestStorage {
        fn new(path: &Path) -> Storage {
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

    impl Drop for TestPath {
        fn drop(&mut self) {
            if self.delete {
                fs::remove_dir_all(&self.path).ok();
            }
        }
    }

    #[test]
    fn storage_can_be_built() {
        let test_path = TestPath::new();

        StorageBuilder::new()
            .column("id", ColumnDatatype::Int32)
            .at(test_path.file_name("test.storage"))
            .unwrap();
    }

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

    #[test]
    fn storage_generates_right_columns() {
        StorageBuilder::new()
            .column("col1", ColumnDatatype::Int32)
            .column("col2", ColumnDatatype::Int32)
            .in_memory()
            .unwrap();
    }

    #[test]
    fn storage_builder_in_valid_path() {
        let mut test_path = TestPath::new();
        let test_file = test_path.file_name("test.storage");
        {
            let builder = StorageBuilder::new()
                .column("id", ColumnDatatype::Int32)
                .at(&test_file);
        }

        // Check that the file exists
        assert!(test_file.metadata().is_ok());
    }

    #[test]
    fn storage_builder_in_invalid_path() {
        let builder = StorageBuilder::new();
        assert!(builder.at("/invalid/path/test.storage").is_err());
        assert!(builder.at("/tmp").is_err());
        assert!(builder.at("/").is_err());
        assert!(builder.at("").is_err());
    }

    #[test]
    #[should_panic(expected="more than once")]
    fn storage_with_duplicated_columns() {
        StorageBuilder::new()
            .column("id", ColumnDatatype::Int32)
            .column("id", ColumnDatatype::Int64)
            .in_memory()
            .unwrap();
    }

    #[test]
    fn a_single_row_can_be_inserted() {
        let mut test_path = TestPath::new();
        test_path.dont_delete();
        let test_file = test_path.file_name("test.storage");

        let mut storage = TestStorage::new(test_file.as_path());

        let lock: Arc<RwLock<Storage>> = Arc::new(RwLock::new(storage));
        {
            let mut inserter = StorageInserter::new(lock.clone());

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

        // Get the Storage outside the Mutex outside the Arc
        // Inception!
        storage = Arc::try_unwrap(lock).ok().unwrap()
            .into_inner().unwrap();

        assert_eq!(storage.num_rows(), 1);

    }

    #[test]
    fn invalid_values_cannot_be_inserted() {
        let test_path = TestPath::new();
        let storage = TestStorage::new(test_path.file_name("test.storage").as_path());

        let lock: Arc<RwLock<Storage>> = Arc::new(RwLock::new(storage));
        {
            let mut inserter = StorageInserter::new(lock.clone());

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
}
