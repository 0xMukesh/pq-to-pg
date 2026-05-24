# pq-to-pg

a fast, minimal tool that converts a directory of parquet files into individual postgres tables

## example

```
go run . --parquet-dir ./data --db-conn postgresql://postgres:postgres@localhost:5432/test_db --num-readers 2 --num-writers 4 --chunk-size 5000
```
