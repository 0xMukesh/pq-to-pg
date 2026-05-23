package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
)

type FileChunk struct {
	Data      [][]any
	TableName string
}

func readPqFile(path string, tableName string, chunkSize int, fileChunksCh chan<- FileChunk) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %s", err)
	}

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %s", err)
	}

	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return fmt.Errorf("failed to read file: %s", err)
	}

	reader := parquet.NewGenericReader[any](pf)
	defer reader.Close()

	buf := make([]parquet.Row, chunkSize)

	for {
		n, err := reader.ReadRows(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("failed to read rows: %s", err)
		}

		if n > 0 {
			chunk := make([][]any, n)
			for i, prow := range buf[:n] {
				row := make([]any, len(prow))
				for j, val := range prow {
					row[j] = val.String()
				}
				chunk[i] = row
			}

			fileChunksCh <- FileChunk{
				Data:      chunk,
				TableName: tableName,
			}
		}
	}

	return nil
}

func sliceToChan[T any](s []T) <-chan T {
	ch := make(chan T)

	go func() {
		for _, item := range s {
			ch <- item
		}
		close(ch)
	}()

	return ch
}
