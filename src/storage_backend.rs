use std::io::{Read, Write, Seek, Cursor};
use std::path::{Path, PathBuf};
use std::fs::OpenOptions;

use ::error::StorageResult;

// ----------------------------------------------------------------------------
pub enum StorageBackend {
    Memory(Cursor<Vec<u8>>),
    File(PathBuf)
}

pub trait BackendReader : Read + Seek {}
impl<T> BackendReader for T where T: Read + Seek {}

pub trait BackendWriter : Write + Seek {}
impl<T> BackendWriter for T where T: Write + Seek {}

impl StorageBackend {
    pub fn new_in_memory() -> StorageBackend {
        StorageBackend::Memory(Cursor::new(Vec::new()))
    }

    pub fn from_existing_memory(vec: Vec<u8>) -> StorageBackend {
        StorageBackend::Memory(Cursor::new(vec))
    }

    pub fn in_path<P: AsRef<Path>>(path: P) -> StorageBackend {
        StorageBackend::File(path.as_ref().to_path_buf())
    }

    pub fn reader<'a>(&'a self) -> StorageResult<Box<BackendReader+'a>> {
        match *self {
            StorageBackend::Memory(ref c) => {
                // Note that the cursor c is immutable. Since a Reader must be mutable, we create
                // a new mutable cursor that "sees" inside the value stored in c, while keeping
                // c read-only.
                let s = c.get_ref().as_slice();
                Ok(Box::new(Cursor::new(s)))
            }
            StorageBackend::File(ref file_path) => {
                let file = try!(OpenOptions::new()
                    .read(true)
                    .write(false)
                    .open(file_path)
                );
                Ok(Box::new(file))
            }
        }
    }

    pub fn writer<'a>(&'a mut self) -> StorageResult<Box<BackendWriter+'a>> {
        match *self {
            StorageBackend::Memory(ref mut c) => Ok(Box::new(c)),
            StorageBackend::File(ref file_path) => {
                let file = try!(OpenOptions::new()
                    .read(true)
                    .write(false)
                    .append(true)
                    .create(true)
                    .open(file_path)
                );
                Ok(Box::new(file))
            }
        }
    }
}

// ----------------------------------------------------------------------------
#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::{Write, Read};

    use ::storage_backend::*;
    use ::test;

    #[test]
    fn read_from_vector() {
        let v: Vec<u8> = vec!(1, 2, 3);
        let backend = StorageBackend::from_existing_memory(v);
        let mut reader = backend.reader().unwrap();

        let mut buffer: [u8; 3] = [0, 0, 0];
        reader.read(&mut buffer).unwrap();
        assert_eq!(buffer, [1, 2, 3]);
    }

    #[test]
    fn write_to_vector() {
        let mut backend = StorageBackend::new_in_memory();
        let orig_buffer: [u8; 3] = [1, 2, 3];

        {
            let mut writer = backend.writer().unwrap();
            writer.write(&orig_buffer).unwrap();
        }

        let mut reader = backend.reader().unwrap();
        let mut buffer: [u8; 3] = [0, 0, 0];
        reader.read(&mut buffer).unwrap();
        assert_eq!(buffer, orig_buffer);
    }

    #[test]
    fn read_from_existing_file() {
        let test_path = test::TestPath::new();
        let file_name = test_path.file_name("test");
        let orig_buffer: [u8; 3] = [1, 2, 3];

        {
            let mut file = File::create(&file_name).unwrap();
            file.write(&orig_buffer).unwrap();
        }

        let backend = StorageBackend::in_path(file_name);
        let mut reader = backend.reader().unwrap();
        let mut buffer: [u8; 3] = [0, 0, 0];
        reader.read(&mut buffer).unwrap();
        assert_eq!(buffer, orig_buffer);
    }

    #[test]
    #[should_panic(expected="No such file or directory")]
    fn read_from_non_existing_file() {
        let test_path = test::TestPath::new();
        let file_name = test_path.file_name("test");
        let backend = StorageBackend::in_path(file_name);

        // This should fail because the file doesn't exist
        backend.reader().unwrap();
    }

    #[test]
    fn write_to_new_file() {
        let test_path = test::TestPath::new();
        let file_name = test_path.file_name("test");
        let orig_buffer: [u8; 3] = [1, 2, 3];
        {
            let mut backend = StorageBackend::File(file_name.clone());
            let mut writer = backend.writer().unwrap();
            writer.write(&orig_buffer).unwrap();
        }

        let mut file = File::open(&file_name).unwrap();
        let mut buffer: [u8; 3] = [0, 0, 0];
        file.read(&mut buffer).unwrap();
        assert_eq!(buffer, orig_buffer);
    }
}
