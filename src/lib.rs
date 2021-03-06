extern crate capnp;
extern crate libc;

pub mod storage;
pub mod storage_inserter;
pub mod error;

mod os;
mod proto_structs;
mod encoding;
mod compression;
mod storage_reader;
mod storage_backend;

#[cfg(test)]
mod test;

#[allow(dead_code)]
mod storage_capnp {
    include!(concat!(env!("OUT_DIR"), "/storage_capnp.rs"));
}
