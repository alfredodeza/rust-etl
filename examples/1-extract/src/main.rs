use polars::prelude::*;
use std::env;

fn load(path: &str) -> PolarsResult<DataFrame> {
    CsvReader::from_path(path)?.has_header(true).finish()
}


// TODO: Potentially explore adding a schema to validate that the
// incoming Dataset is conforming to the expectations of the loader


fn process(df: &DataFrame) {
    println!("{:?}", df);
    println!("{:?}", df.schema());
    // Print data types of columns
    println!("Data Types:");
    for col in df.get_columns() {
        println!("{:?}: {:?}", col.name(), col.dtype());
    }
    let df = drop_empty_columns(df.clone()).unwrap();
    let new_df = replace_newlines(df).unwrap();
    println!("{:?}", new_df);

}

fn drop_empty_columns(df: DataFrame) -> PolarsResult<DataFrame> {
    let mut cols_to_drop = Vec::new();

    for col_name in df.get_column_names() {
        let col = df.column(&col_name).unwrap();

        // Check if the column is not entirely null (not empty)
        if col.null_count() == col.len() {
            cols_to_drop.push(col_name);
            println!("Dropping column: {}", col_name);
        }
    }

    let new_df = df
        .clone()
        .lazy()
        .select([col("*").exclude(cols_to_drop)])
        .collect()?;

    Ok(new_df)
}


fn replace_newlines(df: DataFrame) -> PolarsResult<DataFrame> {

    let new_df = df
        .clone()
        .lazy()
        .with_columns([
            col("notes").str()
                .replace_all(lit(r"\\n|\\r"), lit(" "), false)
                .alias("tmp_notes"),
        ])
        .select([col("*").exclude(&["notes"])])
        .rename(&["tmp_notes"], &["notes"])
        .collect()?;

    Ok(new_df)
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
