extern crate snappy;

use std::fmt;
use std::mem;
use std::slice;
use std::io;
use std::io::BufWriter;

#[derive(Debug, Clone)]
enum Value {
    Null,
    Int(i64),
    Float(f64),
    Raw(Vec<u8>)
}

enum Encoder {
    Flat,
    RLE
}

#[derive(Debug)]
struct EncoderError;

impl Encoder {
    fn encode<T>(&self, values: &[T]) -> Result<Vec<T>, EncoderError> 
        where T: Clone
    {
        let encoded: Vec<T> = match *self {
            Encoder::Flat => Vec::from(values),
            Encoder::RLE => unimplemented!()
        };

        Ok(encoded)
    }
}

impl fmt::Display for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let desc = match *self {
            Encoder::Flat => "Flat",
            Encoder::RLE => "RLE"
        };
        
        write!(f, "{}", desc)
    }
}

#[derive(Debug)]
struct CompressorError;

enum Compressor {
    Raw,
    Snappy
}

impl Compressor {
    fn compress(&self, buffer: &[u8]) -> Result<Vec<u8>, CompressorError> {
        let compressed: Vec<u8> = match *self {
            Compressor::Raw => Vec::from(buffer),
            Compressor::Snappy => snappy::compress(buffer)
        };

        Ok(compressed)
    }
}

impl fmt::Display for Compressor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let desc = match *self {
            Compressor::Raw => "Raw",
            Compressor::Snappy => "Snappy"
        };

        write!(f, "{}", desc)
    }
}

struct BlockGenerator {
    encoder: Encoder,
    compressor: Compressor,
    null_compressor: Compressor,
}

impl BlockGenerator {
    fn new(encoder: Encoder, compressor: Compressor) -> BlockGenerator {
        BlockGenerator {
            encoder: encoder,
            compressor: compressor,
            null_compressor: Compressor::Snappy
        }
    }

    fn generate_block<T>(&self, nulls: &[bool], values: &[T]) -> StorageBlock
        where T: Clone
    {
        let encoded_values = self.encoder.encode(values).unwrap();
        let compressed_values = self.compressor.compress(get_slice_bytes(&encoded_values)).unwrap();
        let compressed_nulls = self.null_compressor.compress(get_slice_bytes(&nulls)).unwrap();
        unimplemented!();
    }
}

struct CompressedBuffer {
    encoding: Encoder,
    compression: Compressor,
    uncompressed_size: usize,
    compressed_data: Vec<u8>
}

impl CompressedBuffer {
    fn new(encoding: Encoder, compression: Compressor, uncompressed_size: usize, values: Vec<u8>) -> CompressedBuffer {
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

trait Serializable: Sized {
    fn serialize(&self, writer: &mut io::Write) -> io::Result<usize>;
    fn unserialize(reader: &mut io::Read) -> io::Result<Self>;
}


#[derive(Debug)]
struct InvalidTypeError;

#[derive(Debug)]
struct InvalidRowError;

trait ColumnGenerator {
    fn append_value(&mut self, value: &Value) -> Result<(), InvalidTypeError>; 
    fn generate_block(&self) -> StorageBlock;
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

struct NullsBitmap {
    packed_bits: Vec<u8>,
    num_values: usize
}

impl NullsBitmap {
    fn new() -> NullsBitmap {
        NullsBitmap {
            packed_bits: Vec::new(),
            num_values: 0
        }
    }

    fn reset(&mut self) {
        self.packed_bits.truncate(0);
        self.num_values = 0;
    }

    fn append_null(&mut self) {
        self.append(false);
    }

    fn append_not_null(&mut self) {
        self.append(true);
    }

    fn append(&mut self, has_value: bool) {
        let bit_offset = self.num_values % 8;

        if bit_offset == 0 {
            self.packed_bits.push(0);
        }

        if has_value {
            let last_byte_offset = self.packed_bits.len()-1;
            let mut last_byte: &mut u8 = unsafe { self.packed_bits.get_unchecked_mut(last_byte_offset) };
            *last_byte |= 1 << bit_offset;
        }

        self.num_values += 1;
    }

    fn get_raw_bits<'a>(&'a self) -> &'a [u8] {
        &self.packed_bits
    }

    fn len(&self) -> usize {
        self.num_values
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

    fn generate_block(&self) -> StorageBlock {
        let raw_bytes: &[u8] = get_slice_bytes(self.values.as_slice());

        let encoding = Encoder::Flat;
        let compression = Compressor::Snappy;

        let compressed_values = CompressedBuffer::new(
            encoding, 
            compression, 
            self.get_raw_size(), 
            snappy::compress(raw_bytes)
        );
        let compressed_nulls = CompressedBuffer::new(
            Encoder::Flat, 
            Compressor::Raw, 
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
    num_rows: usize
}

impl<'a> TableWriter<'a> {
    fn new(block_size: usize, generators: Vec<Box<ColumnGenerator>>, writer: &'a mut io::Write) -> TableWriter<'a> {
        TableWriter {
            block_size: block_size,
            column_generators: generators,
            writer: writer,
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
            let blocks = self.column_generators.iter_mut().map(|g| g.generate_block()).collect::<Vec<_>>();
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

    let block = generator.generate_block();
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

#[test]
fn test_nulls_bitmap() {
    let mut bitmap = NullsBitmap::new();
    assert_eq!(bitmap.len(), 0);

    bitmap.append_null();
    bitmap.append_null();
    bitmap.append_not_null();
    bitmap.append_null();

    {
        let bits = bitmap.get_raw_bits();
        assert_eq!(bitmap.len(), 4);
        assert_eq!(bits.len(), 1);
        assert_eq!(*bits.get(0).unwrap(), 0b00000100);
    }

    bitmap.append_null();
    bitmap.append_not_null();
    bitmap.append_null();
    bitmap.append_not_null();
    // End of first byte

    bitmap.append_not_null();

    {
        let bits = bitmap.get_raw_bits();
        assert_eq!(bitmap.len(), 9);
        assert_eq!(bits.len(), 2);
        assert_eq!(*bits.get(0).unwrap(), 0b10100100);
        assert_eq!(*bits.get(1).unwrap(), 0b00000001);
    }

    bitmap.reset();
    assert_eq!(bitmap.len(), 0);
}


