use std::fs::File;
use std::fmt::Debug;
use std::any::Any;

use arrow::csv;
use arrow::error::Result as ArrowResult;

fn main()-> ArrowResult<()> {
   reader_builder_example()?;

   Ok(())
}

fn reader_builder_example() -> ArrowResult<()>{
    /* d.csv
    "carat","cut","depth"
    0.23,"Ideal",55
    0.21,"Premium",61
     */
    let file = File::open("d.csv").unwrap();
    let csv_builder = csv::ReaderBuilder::new()
        .has_header(true)
        .infer_schema(Some(100));
    let mut csv_reader = csv_builder.build(file)?;
    let batch = csv_reader.next().unwrap().unwrap();
    let schema = batch.schema();
    let num_columns = batch.num_columns();
    for i in 0..num_columns{
        print!("{:} ",schema.field(i).name());
    }
    println!();
    for i in 0..num_columns{
        print!("{:} ",schema.field(i).data_type());
    }
    println!();
    for i in 0..num_columns{
        let col = batch.column(i);
        println!("{:?} ",col);
        println!();
    }

    Ok(())
}
