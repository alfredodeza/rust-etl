# 🦀 Rust for Extract, Transform, and Load operations

_Practice ETL with Rust and Polars_

This repository will walk you through examples for each step in ETL so that you can apply Rust and Polars for these operations using a sample CSV dataset.

You will be using a sample dataset that contains wines from all over the world. Explore [the wine dataset](./top-rated-wines.csv) and familiarize yourself with the data before you start the ETL process.

Each example is a separate Cargo project and it is meant to be run independently. You can run each example by navigating to the project directory and running the following command:

```bash
cargo run ../../top-rated-wines.csv
```

## Lesson 1: Extracting data
For this lesson, you will learn how to read a CSV file and load it into a DataFrame in Polars. You will do minor checking of the data to ensure that it was loaded correctly and that the data is in the expected format.

- [Extract project](./examples/1-extract/)

## Lesson 2: Transforming data
For this lesson, you will learn how to transform the data by filtering out unnecessary columns and rows. You will use one hot encoding to convert columns. There are two examples in this lesson, one that does hot encoding on all columns and another that does hot encoding on selected columns.

- [Transform all columns](./examples/2-transform-dummies/)
- [Transform selected columns](./examples/3-transform-filter/)

## Lesson 3: Loading data
Finally, for this lesson, you will learn how to save the transformed data into a Parquet file. A Parquet file is a columnar storage file that is optimized for reading and writing data. 

- [Load project](./examples/4-load/)

# Extra challenges

1. Verify Parquet file: You will save the transformed data into a Parquet file and then read it back to ensure that the data was saved correctly using the Load project as a reference.
1. Add options for saving: Currently, all projects do not save the CSV back to the file system. Add an option to save the transformed data back to the file system.
1. Add more transformations: Add more transformations to the data such as sorting, grouping, and aggregating data.
1. Implement Schema validation: Use Polars Schema validation to ensure that the data is in the expected format before transforming it.