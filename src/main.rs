use std::fs::File;
use arrow::csv;
use arrow::array::{as_primitive_array, as_string_array, as_null_array};
use arrow::datatypes::*;

fn main() {
   reader_builder_example();
}

fn reader_builder_example(){
    /* d.csv
    "carat","cut","depth"
    0.23,"Ideal",55
    0.21,"Premium",61
     */
    let file = File::open("d.csv").unwrap();
    let csv_builder = csv::ReaderBuilder::new()
        .has_header(true)
        .infer_schema(Some(100));
    let mut csv_reader = csv_builder.build(file).unwrap();
    let record_batch = csv_reader.next().unwrap().unwrap();
    let schema = record_batch.schema();
    let num_rows = record_batch.num_rows();
    let num_columns = record_batch.num_columns();
    println!("# A tv: {} x {}", num_rows, num_columns);
    for i in 0..num_columns{
        print!("{:} ", schema.field(i).name());
    }
    println!();
    for i in 0..num_columns{
        print!("<{}> ", schema.field(i).data_type());
    }
    println!();
    // must be a better way than to read twice
    let file = File::open("d.csv").unwrap();
    let csv_builder = csv::ReaderBuilder::new()
        .has_header(true)
        .infer_schema(Some(100));
    let csv_reader = csv_builder.build(file).unwrap();
    //
    for batch in csv_reader {
        for column in batch.unwrap().columns(){
                match column.data_type() {
                DataType::Int8    => print!("{:?} ",as_primitive_array::<Int8Type>(column).values()),
                DataType::Int16   => print!("{:?} ",as_primitive_array::<Int16Type>(column).values()),
                DataType::Int32   => print!("{:?} ",as_primitive_array::<Int32Type>(column).values()),
                DataType::Int64   => print!("{:?} ",as_primitive_array::<Int64Type>(column).values()),
                DataType::UInt8   => print!("{:?} ",as_primitive_array::<UInt8Type>(column).values()),
                DataType::UInt16  => print!("{:?} ",as_primitive_array::<UInt16Type>(column).values()),
                DataType::UInt32  => print!("{:?} ",as_primitive_array::<UInt32Type>(column).values()),
                DataType::UInt64  => print!("{:?} ",as_primitive_array::<UInt64Type>(column).values()),
                DataType::Float32 => print!("{:?} ",as_primitive_array::<Float32Type>(column).values()),
                DataType::Float64 => print!("{:?} ",as_primitive_array::<Float64Type>(column).values()),
                DataType::Null    => print!("{:?} ",as_null_array(column)),
                DataType::Utf8    => print!("{:} ",as_string_array(column).value(0)),
                _ => println!("NA")
            }
        }
    }
    println!();
}
