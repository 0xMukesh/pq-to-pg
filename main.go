package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
)

type Config struct {
	ParquetDir string
	DbConn     string
	NumReaders int
	NumWriters int
	ChunkSize  int
}

var cfg Config

func main() {
	ctx := context.Background()

	flag.StringVar(&cfg.ParquetDir, "parquet-dir", "", "path to the directory which contains the parquet files")
	flag.StringVar(&cfg.DbConn, "db-conn", "", "connection url to the postgres db")
	flag.IntVar(&cfg.NumReaders, "num-readers", -1, "number of concurrent reader workers to run")
	flag.IntVar(&cfg.NumWriters, "num-writers", -1, "number of concurrent writer workers to run")
	flag.IntVar(&cfg.ChunkSize, "chunk-size", -1, "size of the chunk of parquet file content which the workers read at once")
	flag.Parse()

	switch {
	case cfg.ParquetDir == "":
		log.Fatal("missing --parquet-dir flag")
	case cfg.DbConn == "":
		log.Fatal("missing --db-conn flag")
	case cfg.NumReaders == -1:
		log.Fatal("missing --num-readers flag")
	case cfg.NumWriters == -1:
		log.Fatal("missing --num-writers flag")
	case cfg.ChunkSize == -1:
		log.Fatal("missing --chunk-size flag")
	}

	stat, err := os.Stat(cfg.ParquetDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Fatalf("%s directory does not exist\n", cfg.ParquetDir)
		}

		log.Fatalf("failed to read %s directory: %s\n", cfg.ParquetDir, err)
	}
	if !stat.IsDir() {
		log.Fatalf("%s is not a directory\n", cfg.ParquetDir)
	}

	dbCfg, err := pgxpool.ParseConfig(cfg.DbConn)
	if err != nil {
		log.Fatalf("failed to parse db connection url: %s\n", err)
	}
	dbCfg.MaxConns = int32(cfg.NumWriters)

	pool, err := pgxpool.NewWithConfig(ctx, dbCfg)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %s\n", err)
	}
	if err = pool.Ping(ctx); err != nil {
		log.Fatalf("failed to verify postgres connection: %s\n", err)
	}
	defer pool.Close()

	entries, err := os.ReadDir(cfg.ParquetDir)
	if err != nil {
		log.Fatalf("failed to read %s directory: %s\n", cfg.ParquetDir, err)
	}

	parquetFiles := []string{}
	allowedExts := []string{".pq", ".parquet"}
	for _, entry := range entries {
		if entry.IsDir() || !slices.Contains(allowedExts, filepath.Ext(entry.Name())) {
			continue
		}

		parquetFiles = append(parquetFiles, path.Join(cfg.ParquetDir, entry.Name()))
	}

	pqFileTableNameMapping := make(map[string]string)
	tableNameColsMapping := make(map[string][]string)
	tableNameFieldsMapping := make(map[string][]parquet.Field)

	batch := &pgx.Batch{}

	for _, pqFile := range parquetFiles {
		tableName, _, found := strings.Cut(filepath.Base(pqFile), ".")
		if !found {
			log.Fatalf("%s: failed to compute table name", pqFile)
		}

		schema, err := inferSchema(pqFile)
		if err != nil {
			log.Fatalf("%s: failed to infer schema: %s", pqFile, err)
		}

		cols := []string{}
		for _, v := range schema.Columns() {
			cols = append(cols, v[0])
		}

		pqFileTableNameMapping[pqFile] = tableName
		tableNameColsMapping[tableName] = cols
		tableNameFieldsMapping[tableName] = schema.Fields()

		if err := buildCreateTableQuery(tableName, schema, batch); err != nil {
			log.Fatalf("%s: failed to build create table query: %s", pqFile, err)
		}
	}

	br := pool.SendBatch(ctx, batch)
	defer br.Close()
	if _, err := br.Exec(); err != nil {
		log.Fatalf("failed to create tables: %s", err)
	}

	errCh := make(chan error)

	pqFilesCh := sliceToChan(parquetFiles)
	fileChunksCh := setupReaders(pqFilesCh, errCh, pqFileTableNameMapping, tableNameFieldsMapping)
	doneCh := setupWriters(ctx, fileChunksCh, errCh, pool, tableNameColsMapping)

	go func() {
		<-doneCh
		close(errCh)
	}()

	for err := range errCh {
		fmt.Println(err)
	}
}

func inferSchema(path string) (*parquet.Schema, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %s", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %s", err)
	}

	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %s", err)
	}

	return pf.Schema(), nil
}

func buildCreateTableQuery(tableName string, schema *parquet.Schema, batch *pgx.Batch) error {
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "CREATE TABLE IF NOT EXISTS %s (", tableName)
	for i, field := range schema.Fields() {
		fieldType, ok := pqToPgMapping[field.Type().String()]
		if !ok {
			// manually handle list case
			if field.Type().LogicalType().List != nil {
				elemType := field.Fields()[0].Fields()[0].Type().String() // get the underlying child type
				fieldType = pqToPgMapping[elemType] + "[]"
			} else {
				return fmt.Errorf("attempted type conversion for unimplemented parquet type: %s", field.Type())
			}
		}

		fmt.Fprintf(sb, "%s %s", field.Name(), fieldType)
		if i < len(schema.Fields())-1 {
			sb.WriteString(",")
		}
	}
	sb.WriteString(");")

	batch.Queue(sb.String())
	return nil
}

func setupReaders(
	pqFilesCh <-chan string, errCh chan<- error,
	pqFileTableNameMapping map[string]string, tableNameFieldsMapping map[string][]parquet.Field,
) <-chan FileChunk {
	fileChunksCh := make(chan FileChunk, cfg.NumReaders)
	var wg sync.WaitGroup

	for range cfg.NumReaders {
		wg.Go(func() {
			for item := range pqFilesCh {
				tableName := pqFileTableNameMapping[item]
				fields := tableNameFieldsMapping[tableName]

				if err := readPqFile(item, tableName, fields, cfg.ChunkSize, fileChunksCh); err != nil {
					errCh <- fmt.Errorf("reader error: %s", err)
				}
			}
		})
	}

	go func() {
		wg.Wait()
		close(fileChunksCh)
	}()

	return fileChunksCh
}

func setupWriters(
	ctx context.Context,
	fileChunksCh <-chan FileChunk, errCh chan<- error,
	pool *pgxpool.Pool,
	tableNameColsMapping map[string][]string,
) <-chan any {
	doneCh := make(chan any)
	var wg sync.WaitGroup

	for range cfg.NumWriters {
		wg.Go(func() {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				errCh <- fmt.Errorf("writer error: failed to acquire connection from pool: %s", err)
			}
			defer conn.Release()

			for chunk := range fileChunksCh {
				cols := tableNameColsMapping[chunk.TableName]

				if _, err := conn.Conn().CopyFrom(
					ctx,
					pgx.Identifier{chunk.TableName},
					cols,
					pgx.CopyFromRows(chunk.Data),
				); err != nil {
					errCh <- fmt.Errorf("writer error: failed to copy file chunk: %s", err)
				}
			}
		})
	}

	go func() {
		wg.Wait()
		close(doneCh)
	}()

	return doneCh
}
