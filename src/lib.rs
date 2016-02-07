extern crate capnp;
extern crate libc;

pub mod storage;

mod test;
mod os;

mod table_capnp {
    //include!(concat!(env!("OUT_DIR"), "/table_capnp.rs"));
}
