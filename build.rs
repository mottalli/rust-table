extern crate capnpc;

fn main() {
    ::capnpc::compile("schema", &["src/schema/table.capnp"]).unwrap();
}
