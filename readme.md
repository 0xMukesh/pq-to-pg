# pq-to-pg

a fast and lightweight tool for converting a importing a directory of parquet files into postgresql, with each file mapped to its own table, according to their filename.

## installation 

```
go install github.com/0xmukesh/pq-to-pg@latest
```

## usage

```
pq-to-pg \
  --parquet-dir ./data \
  --db-conn postgresql://postgres:postgres@localhost:5432/test_db \
  --num-readers 3 \
  --num-writers 6 \
  --chunk-size 2000
```

## performance

the following benchmark was executed on a dataset containing approximately 21 GB of parquet files on a machine with the following configuration:
- machine: IdeaPad Slim 5 13ARP10
- cpu: AMD Ryzen 7 7735HS (16) @ 4.83 GHz
- memory: 16 GB RAM
- disk: 512 GB SSD
- go version: 1.26.3
- postgresql version: 18.4

the total time taken for the import process was approximately 15 minutes. the performance may vary depending on the following factors:
- disk throughput
- number of reader/writer workers
- dataset schema complexity
- available system resources
