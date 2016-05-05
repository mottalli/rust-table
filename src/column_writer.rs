extern crate snappy;

use std::fmt;
use std::mem;
use std::slice;
use std::io;
use std::io::BufWriter;

use rustc_serialize::json;

#[derive(Debug, Clone)]
enum Value {
    Null,
    Int(i64),
    Float(f64),
    Raw(Vec<u8>)
}

enum Encoding {
    Flat,
    RLE
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let desc = match *self {
            Encoding::Flat => "Flat",
            Encoding::RLE => "RLE"
        };
        
        write!(f, "{}", desc)
    }
}

enum Compression {
    Raw,
    Snappy
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let desc = match *self {
            Compression::Raw => "Raw",
            Compression::Snappy => "Snappy"
        };

        write!(f, "{}", desc)
    }
}

struct CompressedBuffer {
    encoding: Encoding,
    compression: Compression,
    uncompressed_size: usize,
    compressed_data: Vec<u8>
}

impl CompressedBuffer {
    fn new(encoding: Encoding, compression: Compression, uncompressed_size: usize, values: Vec<u8>) -> CompressedBuffer {
        CompressedBuffer {
            encoding: encoding,
            compression: compression,
            uncompressed_size: uncompressed_size,
            compressed_data: values
        }
    }

    fn get_compressed_size(&self) -> usize { 
        self.compressed_data.len() 
    }

    fn get_compression_rate(&self) -> f32 {
        let compressed_size = self.get_compressed_size();
        self.get_compressed_size() as f32 / self.uncompressed_size as f32
    }
}

impl fmt::Display for CompressedBuffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Compressed buffer: {}, {}, {} bytes ({} uncompressed, {}% compression rate)", 
               self.encoding, 
               self.compression, 
               self.get_compressed_size(), 
               self.uncompressed_size,
               self.get_compression_rate() * 100.0
            )
    }
}

struct StorageBlock {
    compressed_nulls_bitmap: CompressedBuffer,
    compressed_values: CompressedBuffer
}


#[derive(Debug)]
struct InvalidTypeError;

#[derive(Debug)]
struct InvalidRowError;

trait ColumnGenerator {
    fn append_value(&mut self, value: &Value) -> Result<(), InvalidTypeError>; 
    fn encode_values(&self) -> StorageBlock;
    fn get_raw_size(&self) -> usize;
    fn reset(&mut self);
}

trait NativeDatatype {
    type NativeType;

    fn extract_native_value(value: &Value) -> Result<Self::NativeType, InvalidTypeError>;
}

struct NativeColumnGenerator<T: NativeDatatype> {
    nulls: Vec<bool>,
    values: Vec<T::NativeType>
}

impl<T> NativeColumnGenerator<T> 
    where T: NativeDatatype
{
    fn new() -> NativeColumnGenerator<T> {
        NativeColumnGenerator {
            nulls: Vec::new(),
            values: Vec::new()
        }
    }
}

fn extract_native_value_or_null<T: NativeDatatype>(value: &Value) -> Result<Option<T::NativeType>, InvalidTypeError> {
    match *value {
        Value::Null => Ok(None),
        _ => T::extract_native_value(value).map(|v| Some(v))
    }
}

impl NativeDatatype for i32 {
    type NativeType = i32;

    fn extract_native_value(value: &Value) -> Result<Self::NativeType, InvalidTypeError> {
        match *value {
            Value::Int(i) => Ok(i as Self::NativeType),
            _ => Err(InvalidTypeError)
        }
    }
}

fn get_slice_bytes<'a, T>(s: &'a [T]) -> &'a [u8]
    where T: Sized
{
    let ptr = s.as_ptr() as *const u8;
    let size = mem::size_of::<T>() * s.len();
    unsafe { slice::from_raw_parts(ptr, size) }
}

impl<T> ColumnGenerator for NativeColumnGenerator<T> 
    where T: NativeDatatype
{
    fn append_value(&mut self, value: &Value) -> Result<(), InvalidTypeError> {
        extract_native_value_or_null::<T>(value).map(|opt_val| {
            match opt_val {
                None => self.nulls.push(true),
                Some(v) => { self.nulls.push(false); self.values.push(v); }
            }
        })
    }

    fn reset(&mut self) {
        self.nulls.truncate(0);
        self.values.truncate(0);
    }

    fn encode_values(&self) -> StorageBlock {
        let raw_bytes: &[u8] = get_slice_bytes(self.values.as_slice());

        let encoding = Encoding::Flat;
        let compression = Compression::Snappy;

        let compressed_values = CompressedBuffer::new(
            encoding, 
            compression, 
            self.get_raw_size(), 
            snappy::compress(raw_bytes)
        );
        let compressed_nulls = CompressedBuffer::new(
            Encoding::Flat, 
            Compression::Raw, 
            self.nulls.len(), 
            self.nulls.iter().map(|&b| if b {1u8} else {0u8}).collect()
        );

        StorageBlock {
            compressed_nulls_bitmap: compressed_nulls,
            compressed_values: compressed_values
        }

    }

    fn get_raw_size(&self) -> usize {
        self.values.len() * mem::size_of::<T>()
    }
}

struct TableWriter<'a> {
    block_size: usize,
    column_generators: Vec<Box<ColumnGenerator>>,
    writer: &'a mut io::Write,
    encoder: json::Encoder<'a>,
    num_rows: usize
}

impl<'a> TableWriter<'a> {
    fn new(block_size: usize, generators: Vec<Box<ColumnGenerator>>, writer: &'a mut io::Write) -> TableWriter<'a> {
        TableWriter {
            block_size: block_size,
            column_generators: generators,
            writer: writer,
            encoder: json::Encoder::new(&mut writer),
            num_rows: 0
        }
    }

    fn append_row(&mut self, row: Vec<Value>) -> Result<(), InvalidRowError> {
        if row.len() != self.column_generators.len() {
            return Err(InvalidRowError);
        }

        for (gen, value) in self.column_generators.iter_mut().zip(row.iter()) {
            gen.append_value(value).unwrap();
        }

        self.num_rows += 1;

        if self.num_rows % self.block_size == 0 {
            let blocks = self.column_generators.iter_mut().map(|g| g.encode_values()).collect::<Vec<_>>();
        }

        Ok(())
    }
}

#[test]
fn test_new() {
    let mut generator = NativeColumnGenerator::<i32>::new();
    generator.append_value(&Value::Int(42)).unwrap();

    assert_eq!(generator.values.len(), 1);
    assert_eq!(generator.nulls.len(), 1);
    assert_eq!(generator.values[0], 42);
    assert_eq!(generator.nulls[0], false);

    generator.append_value(&Value::Null).unwrap();
    assert_eq!(generator.values.len(), 1);
    assert_eq!(generator.nulls.len(), 2);
    assert_eq!(generator.nulls[1], true);
}

#[test]
fn test_reset() {
    let mut generator = NativeColumnGenerator::<i32>::new();
    generator.append_value(&Value::Int(42)).unwrap();

    assert_eq!(generator.values.len(), 1);
    assert_eq!(generator.nulls.len(), 1);
    
    generator.reset();
    assert_eq!(generator.values.len(), 0);
    assert_eq!(generator.nulls.len(), 0);
}

#[test]
fn test_encoding() {
    let mut generator = NativeColumnGenerator::<i32>::new();

    for i in 0..1_000 {
        generator.append_value(&Value::Int(10)).unwrap();
    }

    let block = generator.encode_values();
}

#[test]
fn test_table_generator() {
    let mut buffer = Vec::<u8>::new();
    let mut buf_writer = BufWriter::new(buffer);

    let generators = {
        let mut v = Vec::<Box<ColumnGenerator>>::new();
        v.push(Box::new(NativeColumnGenerator::<i32>::new()));
        v
    };

    let mut table_writer = TableWriter::new(1_000, generators, &mut buf_writer);

    for block in 0..100 {
        for i in 0..table_writer.block_size {
            let value = block*i;
            let row = vec![Value::Int(value as i64)];

            table_writer.append_row(row).unwrap();
        }
    }

}
