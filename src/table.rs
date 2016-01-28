use ::table_capnp;
use std::path::{Path, PathBuf};
use std::io;

#[derive(Copy, Clone)]
pub enum ColumnDatatype {
    // Basic types suppored by the table backend
    Boolean,
    Byte, Int32, Int64,
    Float, Double,
    UTF8,
    FixedLength(i32),

    // Extended types
    Timestamp, TimestampTZ, Geo
}

pub struct Column {
    name: String,
    datatype: ColumnDatatype,
    nullable: bool,
    is_part_of_pk: bool,
    table_ptr: *const Table
}

impl Column {
    pub fn build(name: &str, datatype: ColumnDatatype) -> ColumnBuilder {
        ColumnBuilder {
            name: String::from(name),
            datatype: datatype,
            nullable: true,
            pk: false
        }
    }
}

#[derive(Clone)]
pub struct ColumnBuilder {
    name: String,
    datatype: ColumnDatatype,
    nullable: bool,
    pk: bool
}

impl ColumnBuilder {
    pub fn null(&mut self) -> &mut Self { self.nullable = true; self }
    pub fn not_null(&mut self) -> &mut Self { self.nullable = false; self }
    pub fn pk(&mut self) -> &mut Self { self.pk = true; self }

    fn create(&self, table: &Table) -> Column {
        Column {
            name: self.name.clone(),
            datatype: self.datatype,
            nullable: self.nullable,
            is_part_of_pk: self.pk,
            table_ptr: unsafe { table as *const Table }
        }
    }
}

pub struct Table {
    name: String,
    num_rows: usize,
    columns: Vec<Column>
}

impl Table {
    pub fn build(name: &str) -> TableBuilder {
        TableBuilder {
            name: String::from(name),
            columns: Vec::new()
        }
    }

    pub fn num_columns(&self) -> usize { self.columns.len() }
}

pub struct TableBuilder {
    name: String,
    columns: Vec<ColumnBuilder>
}

impl TableBuilder {
    pub fn column(&mut self, builder: &ColumnBuilder) -> &mut Self {
        self.columns.push(builder.clone());
        self
    }

    pub fn at<P: AsRef<Path>>(&self, path: P) -> io::Result<Table> {
        let mut table = Table {
            name: self.name.clone(),
            num_rows: 0,
            columns: Vec::new()
        };

        table.columns = self.columns.iter().map(|b| b.create(&table)).collect();
        Ok(table)
    }
}
