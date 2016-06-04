use ::bincode::rustc_serialize::EncodingError;
use ::std::io;
use ::std::convert::From;

#[derive(Debug)]
pub enum TableWriterError {
    EncodingError(EncodingError),
    IoError(io::Error)
}

impl From<EncodingError> for TableWriterError {
    fn from(err: EncodingError) -> TableWriterError {
        TableWriterError::EncodingError(err)
    }
}

impl From<io::Error> for TableWriterError {
    fn from(err: io::Error) -> TableWriterError {
        TableWriterError::IoError(err)
    }
}

pub type TableWriterResult<T> = Result<T, TableWriterError>;
