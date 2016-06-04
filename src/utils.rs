use ::std::{slice, mem};

pub fn get_slice_bytes<'a, T: Sized>(s: &'a [T]) -> &'a [u8]
{
    let ptr = s.as_ptr() as *const u8;
    let size = mem::size_of::<T>() * s.len();
    unsafe { slice::from_raw_parts(ptr, size) }
}

