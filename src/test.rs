#[cfg(test)]
use table;

#[test]
fn table_builder() {
    let table = table::Table::build("test")
        .column(table::Column::build("id", table::ColumnDatatype::Int32).pk())
        .at("/tmp/pepe.table").unwrap();
    assert_eq!(table.num_columns(), 1);
}
