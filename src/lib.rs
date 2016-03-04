extern crate capnp;
extern crate libc;

pub mod storage;

mod os;
mod proto_structs;
mod encoding;
mod compression;

#[cfg(test)]
mod test;

#[allow(dead_code)]
mod storage_capnp {
    include!(concat!(env!("OUT_DIR"), "/storage_capnp.rs"));
}
