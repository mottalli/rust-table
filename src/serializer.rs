extern crate rustc_serialize;

use std::io;
use rustc_serialize::{Encoder};
use std::slice;
use std::mem;

struct ProtocolSerializer<'a> {
    writer: &'a mut io::Write
}

struct ProtocolUnserializer<'a> {
    reader: &'a mut io::Read
}

impl<'a> ProtocolSerializer<'a> {
    fn new(writer: &mut io::Write) -> ProtocolSerializer {
        ProtocolSerializer {
            writer: writer
        }
    }

    fn emit_raw_value<T>(&mut self, value: &T) -> io::Result<()> {
        let value_ptr = value as *const T;
        let value_bytes: &[u8] = unsafe {
            slice::from_raw_parts::<u8>(value_ptr as *const u8, mem::size_of::<T>())
        };

        try!(self.writer.write(value_bytes));

        Ok(())
    }
}

impl<'a> ProtocolUnserializer<'a> {
    fn new(reader: &mut io::Read) -> ProtocolUnserializer {
        ProtocolUnserializer {
            reader: reader
        }
    }

    fn read_raw_value<T>(&mut self) -> io::Result<T> {
        let mut value: T = unsafe {
            let mut value: T = mem::zeroed();
            let mut value_ptr = &mut value as *mut T;
            let value_bytes: &mut [u8] = slice::from_raw_parts_mut(value_ptr as *mut u8, mem::size_of::<T>());

            try!(self.reader.read_exact(value_bytes));

            value
        };

        Ok(value)
    }
}

impl<'a> Encoder for ProtocolSerializer<'a> {
    type Error = io::Error;

    fn emit_usize(&mut self, v: usize) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_u64(&mut self, v: u64) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_u32(&mut self, v: u32) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_u16(&mut self, v: u16) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_u8(&mut self, v: u8) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_isize(&mut self, v: isize) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_i64(&mut self, v: i64) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_i32(&mut self, v: i32) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_i16(&mut self, v: i16) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_i8(&mut self, v: i8) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_bool(&mut self, v: bool) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_f64(&mut self, v: f64) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_f32(&mut self, v: f32) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }
    fn emit_char(&mut self, v: char) -> Result<(), Self::Error>
        { self.emit_raw_value(&v) }


    fn emit_nil(&mut self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_str(&mut self, v: &str) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_enum<F>(&mut self, name: &str, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_enum_variant<F>(&mut self, v_name: &str, v_id: usize, len: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_enum_variant_arg<F>(&mut self, a_idx: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_enum_struct_variant<F>(&mut self, v_name: &str, v_id: usize, len: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_enum_struct_variant_field<F>(&mut self, f_name: &str, f_idx: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_struct<F>(&mut self, name: &str, len: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_struct_field<F>(&mut self, f_name: &str, f_idx: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_tuple<F>(&mut self, len: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_tuple_arg<F>(&mut self, idx: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_tuple_struct<F>(&mut self, name: &str, len: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_tuple_struct_arg<F>(&mut self, f_idx: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_option<F>(&mut self, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_option_none(&mut self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_option_some<F>(&mut self, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_seq<F>(&mut self, len: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_seq_elt<F>(&mut self, idx: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_map<F>(&mut self, len: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_map_elt_key<F>(&mut self, idx: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
    fn emit_map_elt_val<F>(&mut self, idx: usize, f: F) -> Result<(), Self::Error> where F: FnOnce(&mut Self) -> Result<(), Self::Error> { unimplemented!() }
}
