extern crate capnp;
extern crate libc;

pub mod storage;

mod test;
mod os;
mod proto_structs;
mod encoding;
mod compression;

#[allow(dead_code)]
mod storage_capnp {
    include!(concat!(env!("OUT_DIR"), "/storage_capnp.rs"));
}
