extern crate snappy;

use std::fmt;

#[derive(Debug, Clone, RustcEncodable, RustcDecodable)]
pub enum Compressor {
    Raw,
    Snappy
}

impl Compressor {
    pub fn compress(&self, buffer: &[u8]) -> Vec<u8> {
        match *self {
            Compressor::Raw => Vec::from(buffer),
            Compressor::Snappy => snappy::compress(buffer)
        }
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


