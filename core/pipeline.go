package core

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func SetupReaders(filesCh <-chan string, chunksCh chan<- FileChunk, meta map[string]TableMeta, chunkSize int) error {
	for path := range filesCh {
		if err := readParquetFile(path, chunksCh, meta[path], chunkSize); err != nil {
			return fmt.Errorf("reader error: %s", err)
		}
	}

	return nil
}

func SetupWriters(ctx context.Context, chunksCh <-chan FileChunk, pool *pgxpool.Pool, meta map[string]TableMeta, colsByTable map[string][]string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("writer error: failed to acquire connection from pool: %s", err)
	}
	defer conn.Release()

	for chunk := range chunksCh {
		cols := colsByTable[chunk.TableName]

		if _, err := conn.Conn().CopyFrom(ctx, pgx.Identifier{chunk.TableName}, cols, pgx.CopyFromRows(chunk.Data)); err != nil {
			return fmt.Errorf("writer error: %s", err)
		}
	}

	return nil
}
