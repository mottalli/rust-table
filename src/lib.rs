extern crate capnp;

mod test;
mod table;

mod table_capnp {
    include!(concat!(env!("OUT_DIR"), "/table_capnp.rs"));
}

