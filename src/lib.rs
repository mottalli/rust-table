extern crate capnp;
extern crate libc;

pub mod table;

mod test;
mod os;

mod table_capnp {
    //include!(concat!(env!("OUT_DIR"), "/table_capnp.rs"));
}
