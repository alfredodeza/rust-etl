use polars::prelude::*;
use serde::Deserialize;
use std::env;
use std::fs;
use toml;

// Config file format
// example: columns = ["name", "age", "city"]
#[derive(Deserialize)]
struct Config {
    columns: Vec<String>,
}

fn load(path: &str) -> PolarsResult<DataFrame> {
    CsvReader::from_path(path)?.has_header(true).finish()
}

// This function validates the schema of the DataFrame against the expected schema,
// the config file contains the expected schema's column names. (toml format)
// example: columns = ["name", "age", "city"]
fn validate_schema(df: &DataFrame, config_path: &str) -> Result<(), std::io::Error>{
    // Read the config file
    let file_content = fs::read_to_string(config_path).unwrap();

    // Parse the config file
    let config: Config = toml::from_str(&file_content).unwrap();

    // Parse the config file to get the expected column names
    let expected_columns: Vec<&str> = config.columns.iter().map(AsRef::as_ref).collect();

    // Get the actual column names from the DataFrame
    let actual_columns: Vec<&str> = df.get_column_names().into_iter().map(AsRef::as_ref).collect();

    // Compare the expected column names with the actual column names
    match expected_columns == actual_columns {
        true => Ok(()),
        false => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Schema mismatch")),
    }
}

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
        .with_columns([col("notes")
            .str()
            .replace_all(lit(r"\\n|\\r"), lit(" "), false)
            .alias("tmp_notes")])
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
        // Validate the schema of the DataFrame
        Ok(df) => match validate_schema(&df, &args[2]) {
            Ok(_) => process(&df),
            Err(e) => eprintln!("{}", e),
        },
        // Handle errors for loading the CSV file
        Err(e) => eprintln!("{}", e),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;
    use std::fs::File;
    use std::io::Write;

        #[test]
        fn test_drop_empty_columns() -> Result<(), Box<dyn std::error::Error>> {
            // Create a DataFrame with some empty columns
            let df = df![
            "name" => ["John", "Jane"],
            "age" => [Some(30), Some(25)],
            "city" => [None::<&str>, None]
        ]?;

            // Call drop_empty_columns and assert that the empty columns are dropped
            let new_df = drop_empty_columns(df)?;
            assert_eq!(new_df.get_column_names(), &["name", "age"]);

            Ok(())
        }

    #[test]
    fn test_replace_newlines() -> Result<(), Box<dyn std::error::Error>> {
        // Create a DataFrame with a column that contains newline characters
        let df = df!(
        "notes" => ["Hello\nWorld", "Goodbye\rWorld"]
    )?;

        // Call replace_newlines and assert that the newline characters are replaced
        let new_df = replace_newlines(df)?;
        let result = new_df.column("notes").unwrap().get(0);
        match result {
            Ok(value) => assert_eq!(value, polars::prelude::AnyValue::String("Hello World")),
            Err(_) => panic!("Unexpected error!"),
        }

        Ok(())
    }

    #[test]
    fn test_validate_schema() -> Result<(), Box<dyn std::error::Error>> {
        // Create a DataFrame with a known schema
        let df = df![
            "name" => ["John", "Jane"],
            "age" => [30, 25],
            "city" => ["New York", "Los Angeles"]
        ]?;

        // Create a config file with matching column names
        let config_path = "/tmp/config.toml";
        let mut file = File::create(config_path)?;
        write!(file, "columns = [\"name\", \"age\", \"city\"]")?;

        // Call validate_schema and assert that it returns Ok(())
        assert!(validate_schema(&df, config_path).is_ok());

        Ok(())
    }
}