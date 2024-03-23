use polars::prelude::*;
use std::env;

fn load(path: &str) -> PolarsResult<DataFrame> {
    CsvReader::from_path(path)?.has_header(true).finish()
}

fn process(df: &DataFrame) {
    println!("{:?}", df);
    println!("{:?}", df.schema());
    // use rows only in the variety column that are not Red Wine or White Wine
    let df = df.clone().lazy().filter(
        col("variety")
            .eq(lit("Red Wine"))
            .or(col("variety").eq(lit("White Wine"))),
    );

    // now convert using columns_to_dummies()
    let df = df.collect();
    let df = df
        .expect("no df")
        .columns_to_dummies(vec!["variety"], None, false);

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
