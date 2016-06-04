use ::std::fmt;
use ::std::mem;
use ::std::slice;
use ::std::io;
use ::std::io::BufWriter;
use ::bincode::rustc_serialize::{encode_into, EncodingResult};
use ::rustc_serialize::Encodable;
use ::bincode::SizeLimit;

use compression::Compressor;
use encoding::Encoder;
use nulls_bitmap::NullsBitmap;
use file_format;
use result::TableWriterResult;
use utils::get_slice_bytes;

#[derive(Debug, Clone)]
enum Value {
    Null,
    Int(i64),
    Float(f64),
    Raw(Vec<u8>)
}

struct ChunkGenerator {
    encoder: Encoder,
    compressor: Compressor,
    null_compressor: Compressor,
}

impl ChunkGenerator {
    fn new(encoder: Encoder, compressor: Compressor) -> ChunkGenerator {
        ChunkGenerator {
            encoder: encoder,
            compressor: compressor,
            null_compressor: Compressor::Snappy
        }
    }

    fn generate_chunk<T: Sized>(&self, nulls: &NullsBitmap, values: &[T]) -> StorageChunk
    {
        assert_eq!(nulls.len(), values.len());

        let nulls_bits = nulls.get_raw_bits();

        StorageChunk {
            compressed_nulls_bitmap: EncodedCompressedBuffer::from(&Encoder::Flat, &self.null_compressor, &nulls_bits),
            compressed_values: EncodedCompressedBuffer::from(&self.encoder, &self.compressor, values)
        }
    }
}

struct EncodedCompressedBuffer {
    encoder: Encoder,
    compressor: Compressor,
    uncompressed_size: usize,
    compressed_data: Vec<u8>
}

impl EncodedCompressedBuffer {
    fn from<T: Sized>(encoder: &Encoder, compressor: &Compressor, data: &[T]) -> EncodedCompressedBuffer {
        let encoded_values: Vec<u8> = encoder.encode(data);
        let compressed_data: Vec<u8> = compressor.compress(&encoded_values);
        
        EncodedCompressedBuffer {
            encoder: encoder.clone(),
            compressor: compressor.clone(),
            uncompressed_size: data.len(),
            compressed_data: compressed_data
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

impl fmt::Display for EncodedCompressedBuffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Compressed buffer: {}, {}, {} bytes ({} uncompressed, {}% compression rate)", 
               self.encoder, 
               self.compressor, 
               self.get_compressed_size(), 
               self.uncompressed_size,
               self.get_compression_rate() * 100.0
            )
    }
}

struct StorageChunk {
    compressed_nulls_bitmap: EncodedCompressedBuffer,
    compressed_values: EncodedCompressedBuffer
}

impl StorageChunk {
    fn get_total_compressed_size(&self) -> usize {
        [&self.compressed_nulls_bitmap,
            &self.compressed_values]
        .iter()
        .map(|b| b.compressed_data.len())
        //.sum
        .fold(0, |acc, x| acc + x)
    }
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
    fn generate_chunk(&self) -> StorageChunk;
    fn get_raw_size(&self) -> usize;
    fn reset(&mut self);
}

trait NativeDatatype {
    type NativeType: Clone;

    fn extract_native_value(value: &Value) -> Result<Self::NativeType, InvalidTypeError>;
}

struct NativeColumnGenerator<T: NativeDatatype> {
    nulls: NullsBitmap,
    values: Vec<T::NativeType>,
    chunk_generator: ChunkGenerator
}

impl<T> NativeColumnGenerator<T> 
    where T: NativeDatatype
{
    fn new() -> NativeColumnGenerator<T> {
        NativeColumnGenerator {
            nulls: NullsBitmap::new(),
            values: Vec::new(),
            chunk_generator: ChunkGenerator::new(Encoder::Flat, Compressor::Snappy)
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

impl<T> ColumnGenerator for NativeColumnGenerator<T> 
    where T: NativeDatatype
{
    fn append_value(&mut self, value: &Value) -> Result<(), InvalidTypeError> {
        extract_native_value_or_null::<T>(value).map(|opt_val| {
            match opt_val {
                None => self.nulls.append_null(),
                Some(v) => { self.nulls.append_not_null(); self.values.push(v); }
            }
        })
    }

    fn reset(&mut self) {
        self.nulls.reset();
        self.values.truncate(0);
    }

    fn generate_chunk(&self) -> StorageChunk {
        self.chunk_generator.generate_chunk(&self.nulls, &self.values)
    }

    fn get_raw_size(&self) -> usize {
        self.values.len() * mem::size_of::<T>()
    }
}

struct TableWriter<W>
    where W: io::Write + io::Seek
{
    block_size: usize,
    column_generators: Vec<Box<ColumnGenerator>>,
    writer: W,
    num_rows: usize,
    num_rows_in_current_block: usize,
    table_metadata: file_format::TableMetadata
}

static HEADER: [u8; 4] = ['S' as u8, 'N' as u8, 'E' as u8, 'L' as u8];

fn encode_to<T: Encodable, W: io::Write>(writer: &mut W, value: &T) -> EncodingResult<()> {
    encode_into(value, writer, SizeLimit::Infinite)
}

impl<W> TableWriter<W> 
    where W: io::Write + io::Seek
{
    fn new_into(writer: W, block_size: usize, generators: Vec<Box<ColumnGenerator>>) -> io::Result<TableWriter<W>> {
        let mut writer = TableWriter {
            block_size: block_size,
            column_generators: generators,
            writer: writer,
            num_rows: 0,
            num_rows_in_current_block: 0,
            table_metadata: file_format::TableMetadata::new()
        };

        try!(writer.write_signature());
        Ok(writer)
    }

    fn write_signature(&mut self) -> io::Result<()> {
        try!(self.writer.write(&HEADER));
        Ok(())
    }

    fn write_footer(&mut self) -> TableWriterResult<()> {
        let current_offset: usize = self.current_offset();
        try!(encode_to(&mut self.writer, &self.table_metadata));
        try!(encode_to(&mut self.writer, &current_offset));
        try!(self.write_signature());

        Ok(())
    }

    pub fn finalize(mut self) -> TableWriterResult<W> {
        try!(self.flush_block());
        try!(self.write_footer());
        Ok(self.writer)
    }

    fn append_row(&mut self, row: &Vec<Value>) -> Result<(), InvalidRowError> {
        if row.len() != self.column_generators.len() {
            return Err(InvalidRowError);
        }

        for (gen, value) in self.column_generators.iter_mut().zip(row.iter()) {
            gen.append_value(value).unwrap();
        }

        self.num_rows += 1;
        self.num_rows_in_current_block += 1;

        if self.num_rows % self.block_size == 0 {
            self.flush_block();
        }

        Ok(())
    }

    fn current_offset(&mut self) -> usize {
        self.writer.seek(io::SeekFrom::Current(0)).unwrap() as usize
    }

    fn flush_block(&mut self) -> io::Result<()> {
        if self.num_rows_in_current_block == 0 {
            return Ok(());
        }

        let chunks: Vec<StorageChunk> = self.column_generators.iter_mut().map(|g| g.generate_chunk()).collect::<Vec<_>>();

        let chunks_metadata: Vec<file_format::ChunkMetadata> = chunks.iter().map(|chunk| {
            let chunk_header = file_format::ChunkHeader {
                nulls_encoder: chunk.compressed_nulls_bitmap.encoder.clone(),
                nulls_compressor: chunk.compressed_nulls_bitmap.compressor.clone(),
                nulls_size: chunk.compressed_nulls_bitmap.compressed_data.len(),
                values_encoder: chunk.compressed_values.encoder.clone(),
                values_compressor: chunk.compressed_values.compressor.clone(),
                values_size: chunk.compressed_values.compressed_data.len()
            };

            let current_offset = self.current_offset();

            encode_to(&mut self.writer, &chunk_header).expect("Unable to write chunk header");
            self.writer.write(&chunk.compressed_nulls_bitmap.compressed_data).expect("Unable to write null values");
            self.writer.write(&chunk.compressed_values.compressed_data).expect("Unable to write values");

            file_format::ChunkMetadata {
                file: None,
                offset_in_file: current_offset
            }
        }).collect();

        self.table_metadata.blocks.push(file_format::BlockMetadata {
            num_rows_in_block: self.num_rows_in_current_block,
            chunks: chunks_metadata
        });

        // Reset the current block
        self.num_rows_in_current_block = 0;
        for generator in self.column_generators.iter_mut() {
            generator.reset();
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
    //assert_eq!(generator.nulls[0], false);

    generator.append_value(&Value::Null).unwrap();
    assert_eq!(generator.values.len(), 1);
    assert_eq!(generator.nulls.len(), 2);
    //assert_eq!(generator.nulls[1], true);
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
fn test_table_generator() {
    use std::io::{Seek, Read};

    let mut buffer = Vec::<u8>::new();
    let mut buf_writer = io::Cursor::new(buffer);

    let generators = {
        let mut v = Vec::<Box<ColumnGenerator>>::new();
        v.push(Box::new(NativeColumnGenerator::<i32>::new()));
        v
    };

    let mut table_writer = TableWriter::new_into(buf_writer, 1_000, generators).unwrap();
    let num_blocks = 10;

    for block in 0..num_blocks {
        for i in 0..table_writer.block_size {
            let value = block*i;
            let row = vec![Value::Int(value as i64)];

            table_writer.append_row(&row).unwrap();
        }
    }

    assert_eq!(table_writer.table_metadata.blocks.len(), num_blocks);

    table_writer.append_row(&vec![Value::Int(10)]).unwrap();
    assert_eq!(table_writer.num_rows_in_current_block, 1);

    let mut table: io::Cursor<Vec<u8>> = table_writer.finalize().unwrap();
    let mut buffer: [u8; 4] = [0; 4];

    table.seek(io::SeekFrom::Start(0)).unwrap();
    table.read_exact(&mut buffer).unwrap();
    assert_eq!(buffer, HEADER);

    table.seek(io::SeekFrom::End(-4)).unwrap();
    table.read_exact(&mut buffer).unwrap();
    assert_eq!(buffer, HEADER);
}

#[test]
fn test_storage_chunk_size() {
    // Makes sure the storage chunks report the right size
    let chunk = StorageChunk {
        compressed_nulls_bitmap: 
            EncodedCompressedBuffer::from(&Encoder::Flat, &Compressor::Raw, &vec![1u8,2,3]),
        compressed_values: 
            EncodedCompressedBuffer::from(&Encoder::Flat, &Compressor::Raw, &vec![4u8,5])
    };

    assert_eq!(chunk.get_total_compressed_size(), 5);

    let chunk = StorageChunk {
        compressed_nulls_bitmap: 
            EncodedCompressedBuffer::from(&Encoder::Flat, &Compressor::Raw, &vec![1i32,2,3]),
        compressed_values: 
            EncodedCompressedBuffer::from(&Encoder::Flat, &Compressor::Raw, &vec![4,5])
    };

    assert_eq!(chunk.get_total_compressed_size(), 5*4);
}
