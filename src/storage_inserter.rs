use std::mem;
use std::slice;
use std::sync::{Arc, RwLock};
use std::io;
use std::io::{Write};

use capnp::message::{Builder as ProtoBuilder};

use ::encoding::Encoding;
use ::compression::Compression;
use ::storage::{ColumnDatatype, Storage, ColumnValue, StorageResult, StorageError, NumericValue};
use ::proto_structs;
use ::proto_structs::ProtocolBuildable;

// ----------------------------------------------------------------------------
pub struct EncodedChunk<'a>(pub Encoding, pub &'a [u8]);
pub struct CompressedChunk<'a>(pub Compression, pub Encoding, pub &'a [u8]);

// ----------------------------------------------------------------------------
/// Helper function
fn get_slice_bytes<'a, T>(s: &'a [T]) -> &'a [u8]
    where T: Sized
{
    let ptr = s.as_ptr() as *const u8;
    let size = mem::size_of::<T>() * s.len();
    unsafe { slice::from_raw_parts(ptr, size) }
}


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
        self.encoded_chunk_buffer.write(&nulls).unwrap();
        self.encoded_chunk_buffer.write(&self.values).unwrap();

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
                _ => unreachable!()
            }
        }
    }

    fn get_encoded_chunk<'a>(&'a mut self) -> EncodedChunk<'a> {
        self.encoded_chunk_buffer.clear();
        self.encoded_chunk_buffer.write(get_slice_bytes(&self.sizes)).unwrap();
        self.encoded_chunk_buffer.write(&self.values).unwrap();

        EncodedChunk(Encoding::Raw, &self.encoded_chunk_buffer)
    }

    fn reset(&mut self) {
        self.sizes.clear();
        self.values.clear();
    }
}

// ----------------------------------------------------------------------------
/// Responsible for creating several instances of StorageInserter.
/// This allows us to insert rows concurrently into a storage.
pub struct InsertionManager {
    storage_lock: Arc<RwLock<Storage>>
}

impl InsertionManager {
    pub fn new(storage: Storage) -> InsertionManager {
        InsertionManager {
            storage_lock: Arc::new(RwLock::new(storage))
        }
    }

    pub fn create_inserter(&mut self) -> StorageInserter {
        StorageInserter::new(self.storage_lock.clone())
    }

    pub fn finish_inserting(self) -> StorageResult<Storage> {
        let mut storage = Arc::try_unwrap(self.storage_lock)
            .ok().expect("Tried to finish inserting rows while there are pending insertions")
            .into_inner().unwrap();

        try!(storage.write_footer());
        Ok(storage)
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
    fn new(storage: Arc<RwLock<Storage>>) -> StorageInserter {
        let (max_rows_in_stripe, chunk_generators) = {
            // Acquire read lock
            let storage = storage.read().unwrap();

            let max_rows_in_stripe = Self::num_rows_in_stripe_hint(&storage);
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
 
    /// A hint for how many rows should fit in a storage stripe
    fn num_rows_in_stripe_hint(storage: &Storage) -> usize {
        let disk_block_size: usize = 4096;
        // How many blocks in a stripe
        let blocks_in_stripe: usize = 64;

        // Find, for all the numeric columns, the one with the biggest size.
        let max_size = storage.columns.iter()
            .filter(|c| c.datatype_info.is_numeric)
            .map(|c| c.datatype_info.value_size.unwrap())
            .max().unwrap_or(1);    // If there are no numeric colums, assume size 1

        (blocks_in_stripe*disk_block_size) / max_size
    }


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

                try!(Self::append_stripe(&mut storage, self.enqueued_rows.len(), &encoded_stripe));
            }

            for chunk_generator in self.chunk_generators.iter_mut() {
                chunk_generator.reset();
            }
        }

        self.enqueued_rows.clear();
        Ok(())
    }

    fn append_stripe(storage: &mut Storage, num_rows: usize, stripe: &Vec<EncodedChunk>) -> StorageResult<()> {
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
        let stripe_size: usize = compressed_chunks.iter().map(|&CompressedChunk(_, _, c)| c.len()).fold(0, |a, b| a + b);

        // Get the current offset in the storage's backend
        let stripe_header_absolute_offset = storage.backend.seek(io::SeekFrom::Current(0)).unwrap() as usize;

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
            try!(::capnp::serialize::write_message(&mut storage.backend, &builder));
        }

        // Now write all the compressed columns
        for &CompressedChunk(_, _, chunk) in compressed_chunks.iter() {
            try!(storage.backend.write(chunk));
        }

        storage.append_stripe(&proto_structs::Stripe {
            absolute_offset: stripe_header_absolute_offset,
            num_rows: num_rows
        });

        Ok(())
    }

}

impl Drop for StorageInserter
{
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

