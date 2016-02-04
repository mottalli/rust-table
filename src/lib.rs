extern crate capnp;
extern crate libc;

mod test;
mod table;
mod os;
mod raw_ptr;

mod table_capnp {
    include!(concat!(env!("OUT_DIR"), "/table_capnp.rs"));
}

