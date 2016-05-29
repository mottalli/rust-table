use std::io::{Write, Read};
use bincode::SizeLimit;

use encoding::Encoder;
use compression::Compressor;

#[derive(RustcEncodable, RustcDecodable)]
pub struct ChunkHeader {
    pub nulls_encoder: Encoder,
    pub nulls_compressor: Compressor,
    pub nulls_size: usize,
    pub values_encoder: Encoder,
    pub values_compressor: Compressor,
    pub values_size: usize,
}

#[derive(RustcEncodable, RustcDecodable)]
pub struct TableMetadata {
    pub num_rows: usize,
    pub blocks: Vec<BlockMetadata>
}

impl TableMetadata {
    pub fn new() -> TableMetadata {
        TableMetadata {
            num_rows: 0,
            blocks: Vec::new()
        }
    }
}

#[derive(RustcEncodable, RustcDecodable)]
pub struct BlockMetadata {
    pub num_rows_in_block: usize,
    pub chunks: Vec<ChunkMetadata>
}

#[derive(RustcEncodable, RustcDecodable)]
pub struct ChunkMetadata {
    pub file: Option<String>,
    pub offset_in_file: usize
}

//#[derive(PartialEq, Debug, RustcEncodable, RustcDecodable)]
/*
#[derive(PartialEq, Debug, RustcEncodable, RustcDecodable)]
enum Bar {
    BAR, BAZ
}

#[derive(RustcEncodable, RustcDecodable)]
struct Foo {
    x: i16,
    y: f32,
    z: Vec<i8>,
    aa: Bar
}

#[test]
fn test_bincode() {
    let foo = Foo { x: 12, y: 34.56, z: vec![1,2,3], aa: Bar::BAR };

    let mut buffer: Vec<u8> = Vec::new();

    encode_into(&foo, &mut buffer, SizeLimit::Infinite).unwrap();

    let decoded: Foo = decode_from(&mut buffer.as_slice(), SizeLimit::Infinite).unwrap();
    assert_eq!(decoded.x, 12);
    assert_eq!(decoded.y, 34.56);
    assert_eq!(decoded.z, vec![1,2,3]);
    assert_eq!(decoded.aa, Bar::BAR);
}
*/
