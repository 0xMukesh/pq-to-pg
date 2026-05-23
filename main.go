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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
)

type Config struct {
	ParquetDir string
	DbConn     string
	NumWorkers int
	ChunkSize  int
}

var cfg Config

func main() {
	ctx := context.Background()

	flag.StringVar(&cfg.ParquetDir, "parquet-dir", "", "path to the directory which contains the parquet files")
	flag.StringVar(&cfg.DbConn, "db-conn", "", "connection url to the postgres db")
	flag.IntVar(&cfg.NumWorkers, "num-workers", -1, "number of concurrent workers to run")
	flag.IntVar(&cfg.ChunkSize, "chunk-size", -1, "size of the chunk of parquet file content which the workers read at once")
	flag.Parse()

	switch {
	case cfg.ParquetDir == "":
		log.Fatal("missing --parquet-dir flag")
	case cfg.DbConn == "":
		log.Fatal("missing --db-conn flag")
	case cfg.NumWorkers == -1:
		log.Fatal("missing --num-workers flag")
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

	pool, err := pgxpool.New(ctx, cfg.DbConn)
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

	// capturing all the parquet files at the root level
	parquetFiles := []string{}
	allowedExts := []string{".pq", ".parquet"}
	for _, entry := range entries {
		if entry.IsDir() || !slices.Contains(allowedExts, filepath.Ext(entry.Name())) {
			continue
		}

		parquetFiles = append(parquetFiles, path.Join(cfg.ParquetDir, entry.Name()))
	}

	// creating tables for each of the file
	// spinning one goroutine per file
	var wg sync.WaitGroup
	var mu sync.Mutex
	createTableErrs := []error{}

	for _, pqFile := range parquetFiles {
		wg.Go(func() {
			if err = createTable(ctx, pqFile, pool); err != nil {
				mu.Lock()
				defer mu.Unlock()
				createTableErrs = append(createTableErrs, err)
			}
		})
	}
	wg.Wait()

	for i, err := range createTableErrs {
		log.Printf("%s: %s", parquetFiles[i], err.Error())
	}
	if len(createTableErrs) != 0 {
		log.Fatal("error: failed to create table with the errors above")
	}
}

func createTable(ctx context.Context, path string, pool *pgxpool.Pool) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %s", err)
	}

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %s", err)
	}

	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return fmt.Errorf("failed to open file: %s", err)
	}

	schema := pf.Schema()
	tableName, _, found := strings.Cut(filepath.Base(path), ".")
	if !found {
		return fmt.Errorf("failed to compute table name")
	}

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

	if _, err = pool.Exec(ctx, sb.String()); err != nil {
		return fmt.Errorf("failed to create the table %s: %s", tableName, err)
	}

	return nil
}
