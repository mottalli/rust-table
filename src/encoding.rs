use std::fmt;

#[derive(Debug, Clone, RustcEncodable, RustcDecodable)]
pub enum Encoder {
    Flat,
    RLE
}

#[derive(Debug)]
pub struct EncoderError;

impl Encoder {
    pub fn encode<T>(&self, values: &[T]) -> Result<Vec<T>, EncoderError> 
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

