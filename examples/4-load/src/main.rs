use polars::prelude::*;
use std::env;

fn load(path: &str) -> PolarsResult<DataFrame> {
    CsvReader::from_path(path)?.has_header(true).finish()
}

fn process(df: &mut DataFrame) {
    let mut file = std::fs::File::create("wine-ratings.parquet").unwrap();
    ParquetWriter::new(&mut file).finish(df).unwrap();
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <path to csv>", args[0]);
        std::process::exit(1);
    }

    let csv_path = &args[1];
    match load(csv_path) {
        Ok(mut df) => process(&mut df),
        Err(e) => eprintln!("{}", e),
    }
}
