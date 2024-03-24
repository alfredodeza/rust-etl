use polars::prelude::*;
use std::env;

fn load(path: &str) -> PolarsResult<DataFrame> {
    CsvReader::from_path(path)?.has_header(true).finish()
}

fn process(df: &DataFrame) {
    println!("{:?}", df);
    println!("{:?}", df.schema());
    // do one hot encoding to the dataset
    let df = df.to_dummies(None, false);

    // check the output, how do the columns look?
    println!("{:?}", df);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <path to csv>", args[0]);
        std::process::exit(1);
    }

    let csv_path = &args[1];
    match load(csv_path) {
        Ok(df) => process(&df),
        Err(e) => eprintln!("{}", e),
    }
}
