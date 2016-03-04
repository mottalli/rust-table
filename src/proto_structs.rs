use ::storage_capnp::stripe_header::Builder as StripeHeaderBuilder;
use ::storage_capnp::column_chunk_header::Builder as ColumnChunkHeaderBuilder;

use ::encoding::Encoding;
use ::compression::Compression;

// ----------------------------------------------------------------------------
pub trait ProtocolBuildable<'a> {
    type Builder: ::capnp::traits::FromPointerBuilder<'a>;

    fn build_message(&self, builder: &mut Self::Builder);

    /* This is a lifetime hell and I can never get it right.
    fn write_message<W>(&self, w: &mut W) -> io::Result<()>
        where W: io::Write
    {
        let mut builder = ProtoBuilder::new_default();
        {
            let mut header_builder: Self::Builder = builder.init_root::<Self::Builder>();
            self.build_message(&mut header_builder);
        }
        ::capnp::serialize::write_message(w, &builder)
    }
    */
}

// ----------------------------------------------------------------------------
/// This is the translation of Capnp's structs to Rust.
pub struct ColumnChunkHeader {
    pub relative_offset: usize,
    pub compressed_size: usize,
    pub uncompressed_size: usize,
    pub encoding: Encoding,
    pub compression: Compression,
}

pub struct StripeHeader {
    pub num_rows: usize,
    pub column_chunks: Vec<ColumnChunkHeader>,
    pub stripe_size: usize
}

pub struct Stripe {
    pub absolute_offset: usize,
    pub num_rows: usize
}

impl<'a> ProtocolBuildable<'a> for StripeHeader {
    type Builder = StripeHeaderBuilder<'a>;

    fn build_message(&self, builder: &mut Self::Builder) {
        builder.set_num_rows(self.num_rows as u32);
        builder.set_stripe_size(self.stripe_size as u64);
        let mut column_chunks_builder = builder.borrow().init_column_chunks(self.column_chunks.len() as u32);
        for (c, column_chunk) in self.column_chunks.iter().enumerate() {
            let mut column_chunk_builder = column_chunks_builder.borrow().get(c as u32);
            column_chunk.build_message(&mut column_chunk_builder);
        }
    }
}

impl<'a> ProtocolBuildable<'a> for ColumnChunkHeader {
    type Builder = ColumnChunkHeaderBuilder<'a>;

    fn build_message(&self, builder: &mut Self::Builder) {
        builder.set_relative_offset(self.relative_offset as u64);
        builder.set_compressed_size(self.compressed_size as u32);
        builder.set_uncompressed_size(self.uncompressed_size as u32);
        builder.set_encoding(match self.encoding {
            Encoding::Raw => ::storage_capnp::Encoding::Raw,
            Encoding::Delta => ::storage_capnp::Encoding::Delta,
            Encoding::RLE => ::storage_capnp::Encoding::Rle
        });
        builder.set_compression(match self.compression {
            Compression::None => ::storage_capnp::Compression::None,
            Compression::Snappy => ::storage_capnp::Compression::Snappy,
        });
    }
}
