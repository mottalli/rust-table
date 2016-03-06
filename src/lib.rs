extern crate capnp;
extern crate libc;

pub mod storage;
pub mod storage_inserter;

mod os;
mod proto_structs;
mod encoding;
mod compression;
mod storage_reader;

#[cfg(test)]
mod test;

#[allow(dead_code)]
mod storage_capnp {
    include!(concat!(env!("OUT_DIR"), "/storage_capnp.rs"));
}
