use ::std::fmt;

use utils::get_slice_bytes;

#[derive(Debug, Clone, RustcEncodable, RustcDecodable)]
pub enum Encoder {
    Flat,
    RLE
}

impl Encoder {
    pub fn encode<T: Sized>(&self, values: &[T]) -> Vec<u8>
    {
        match *self {
            Encoder::Flat => {
                // Copy the raw bytes
                let bytes: &[u8] = get_slice_bytes(values);
                Vec::from(bytes)

            },
            Encoder::RLE => unimplemented!()
        }
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

