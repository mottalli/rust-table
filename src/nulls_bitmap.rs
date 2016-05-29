pub struct NullsBitmap {
    packed_bits: Vec<u8>,
    num_values: usize
}

impl NullsBitmap {
    pub fn new() -> NullsBitmap {
        NullsBitmap {
            packed_bits: Vec::new(),
            num_values: 0
        }
    }

    pub fn reset(&mut self) {
        self.packed_bits.truncate(0);
        self.num_values = 0;
    }

    pub fn append_null(&mut self) {
        self.append(false);
    }

    pub fn append_not_null(&mut self) {
        self.append(true);
    }

    pub fn append(&mut self, has_value: bool) {
        let bit_offset = self.num_values % 8;

        if bit_offset == 0 {
            self.packed_bits.push(0);
        }

        if has_value {
            let last_byte_offset = self.packed_bits.len()-1;
            let mut last_byte: &mut u8 = unsafe { self.packed_bits.get_unchecked_mut(last_byte_offset) };
            *last_byte |= 1 << bit_offset;
        }

        self.num_values += 1;
    }

    pub fn get_raw_bits<'a>(&'a self) -> &'a [u8] {
        &self.packed_bits
    }

    pub fn len(&self) -> usize {
        self.num_values
    }
}

#[test]
fn test_nulls_bitmap() {
    let mut bitmap = NullsBitmap::new();
    assert_eq!(bitmap.len(), 0);

    bitmap.append_null();
    bitmap.append_null();
    bitmap.append_not_null();
    bitmap.append_null();

    {
        let bits = bitmap.get_raw_bits();
        assert_eq!(bitmap.len(), 4);
        assert_eq!(bits.len(), 1);
        assert_eq!(*bits.get(0).unwrap(), 0b00000100);
    }

    bitmap.append_null();
    bitmap.append_not_null();
    bitmap.append_null();
    bitmap.append_not_null();
    // End of first byte

    bitmap.append_not_null();

    {
        let bits = bitmap.get_raw_bits();
        assert_eq!(bitmap.len(), 9);
        assert_eq!(bits.len(), 2);
        assert_eq!(*bits.get(0).unwrap(), 0b10100100);
        assert_eq!(*bits.get(1).unwrap(), 0b00000001);
    }

    bitmap.reset();
    assert_eq!(bitmap.len(), 0);
}
