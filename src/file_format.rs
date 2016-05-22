use std::io::{Write, Read};
use bincode::rustc_serialize::{encode_into, decode_from};
use bincode::SizeLimit;

#[derive(RustcEncodable, RustcDecodable)]
struct Foo {
    x: i16,
    y: f32,
    z: Vec<i8>
}

#[test]
fn test_bincode() {
    let foo = Foo { x: 12, y: 34.56, z: vec![1,2,3] };

    let mut buffer: Vec<u8> = Vec::new();

    encode_into(&foo, &mut buffer, SizeLimit::Infinite).unwrap();

    let decoded: Foo = decode_from(&mut buffer.as_slice(), SizeLimit::Infinite).unwrap();
    assert_eq!(decoded.x, 12);
    assert_eq!(decoded.y, 34.56);
    assert_eq!(decoded.z, vec![1,2,3]);
}
