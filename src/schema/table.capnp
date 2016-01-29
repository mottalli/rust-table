@0xfe52ae551973a120;        # Generated by `capnp id`

struct Table {
    version @0 :Int16;
    numRows @1 :Int64;
    columns @2 :List(Column);
    sortColumn @3 :SortColumn;
    chunks @4: List(Chunk);
    metadata @5 :List(Metadata);

    struct Chunk {
        size @0 :Int64;
        numRows @1 :Int64;
        chunkColumns @2 :List(ColumnChunk);
    }

    struct ColumnChunk {
        encoding @0 :Encoding;
        compression @1 :Compression;
    }

    enum Encoding {
        raw @0;
        delta @1;
    }

    enum Compression {
        none @0;
        snappy @1;
    }

    struct SortColumn {
        columnIdx @0 :Int32;
        ascending @1 :Bool = true;
    }

    struct Metadata {
        key @0 :Text;
        value @1 :Data;
    }

    struct Column {
        name @0 :Text;
        type @1 :ColumnType;

        # Only for type = fixed_len
        size @2 :Int32 = 0;

        nullable @3 :Bool = true;
        primaryKey @4 :Bool = false;
        indexed @5 :Bool = false;

        metadata @6 :List(Metadata);

        enum ColumnType {
            boolean @0;
            byte @1;
            int32 @2;
            int64 @3;
            float @4;
            double @5;
            utf8 @6;
            fixedLen @7;
        }
    }
}
