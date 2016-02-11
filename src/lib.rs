extern crate capnp;
extern crate libc;

pub mod storage;

mod test;
mod os;

mod storage_capnp {
    include!(concat!(env!("OUT_DIR"), "/storage_capnp.rs"));
}
