use std::path::PathBuf;
use std::io;

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
