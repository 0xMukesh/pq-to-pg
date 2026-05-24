package main

import (
	"context"
	"log"
	"sync"

	"github.com/0xmukesh/pq-to-pg/core"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.Background()

	cfg, err := core.ParseConfig()
	if err != nil {
		log.Fatalf("failed to parse config: %s\n", err)
	}

	pool, err := core.ConnectToDb(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %s\n", err)
	}
	defer pool.Close()

	files, err := core.CollectFiles(cfg.ParquetDir)
	if err != nil {
		log.Fatalf("failed to collect files: %s\n", err)
	}

	tableMetas, err := core.BuildTableMetas(files) // {file_path: meta}
	if err != nil {
		log.Fatalf("failed to build table metas: %s\n", err)
	}
	colsByTable := make(map[string][]string, len(tableMetas))
	for _, m := range tableMetas {
		colsByTable[m.Name] = m.Cols
	}

	if err := core.CreateTables(ctx, pool, tableMetas); err != nil {
		log.Fatalf("failed to create tables: %s\n", err)
	}

	filesCh := core.SliceToChan(files)
	chunksCh := make(chan core.FileChunk, cfg.NumReaders*cfg.NumWriters)
	g, ctx := errgroup.WithContext(ctx)

	var readerWg sync.WaitGroup
	for range cfg.NumReaders {
		readerWg.Add(1)
		g.Go(func() error {
			defer readerWg.Done()
			return core.SetupReaders(filesCh, chunksCh, tableMetas, cfg.ChunkSize)
		})
	}

	go func() {
		readerWg.Wait()
		close(chunksCh)
	}()

	for range cfg.NumWriters {
		g.Go(func() error {
			return core.SetupWriters(ctx, chunksCh, pool, tableMetas, colsByTable)
		})
	}

	if err := g.Wait(); err != nil {
		log.Fatalf("pipeline error: %s", err)
	}
}
