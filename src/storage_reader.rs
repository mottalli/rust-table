use std::iter::Iterator;

use ::storage::Storage;
use ::proto_structs;

pub struct StorageStripeIterator<'a> {
    current_stripe: usize,
    storage: &'a Storage
}

pub struct StripeReference<'a> {
    storage: &'a Storage,
    stripe: proto_structs::Stripe
}

/*
impl<'a> StripeReference<'a> {
    pub fn get_header(&mut self)
}
*/

impl<'a> Iterator for StorageStripeIterator<'a> {
    type Item = StripeReference<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.current_stripe >= self.storage.stripes.len() {
            None
        } else {
            Some(StripeReference {
                storage: self.storage,
                stripe: self.storage.stripes[self.current_stripe].clone()
            })
        };

        self.current_stripe += 1;
        result
    }
}
