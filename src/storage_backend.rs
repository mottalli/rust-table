use std::io::{Read, Write};
use std::path::PathBuf;

// ----------------------------------------------------------------------------
pub enum StorageBackend {
    Memory(Vec<u8>),
    File(PathBuf)
}

impl StorageBackend {
    pub fn reader<'a>(&'a self) -> Box<Read+'a> {
        match *self {
            StorageBackend::Memory(ref v) => Box::new(v.as_slice()),
            _ => unimplemented!()
        }
    }

    pub fn writer<'a>(&'a mut self) -> Box<&'a mut Write> {
        match *self {
            StorageBackend::Memory(ref mut v) => Box::new(v),
            _ => unimplemented!()
        }
    }
}

// ----------------------------------------------------------------------------
#[cfg(test)]
mod test {
    use storage_backend::*;

    #[test]
    fn read_from_vector() {
        let v: Vec<u8> = vec!(1, 2, 3);
        let backend = StorageBackend::Memory(v);
        let mut reader = backend.reader();
        
        let mut buffer: [u8; 3] = [0, 0, 0];
        reader.read(&mut buffer).unwrap();
        assert_eq!(buffer, [1, 2, 3]);
    }

    #[test]
    fn write_to_vector() {
        let mut backend = StorageBackend::Memory(Vec::<u8>::new());
        let orig_buffer: [u8; 3] = [1, 2, 3];

        {
            let mut writer = backend.writer();
            writer.write(&orig_buffer).unwrap();
        }
        
        let mut reader = backend.reader();
        let mut buffer: [u8; 3] = [0, 0, 0];
        reader.read(&mut buffer).unwrap();
        assert_eq!(buffer, orig_buffer);
    }
}
