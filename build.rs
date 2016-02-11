extern crate capnpc;

fn main() {
    ::capnpc::compile("schema", &["src/schema/storage.capnp"]).unwrap();
}
