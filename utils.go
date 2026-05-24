package main

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
)

type FileChunk struct {
	Data      [][]any
	TableName string
}

func readPqFile(path string, tableName string, fields []parquet.Field, chunkSize int, fileChunksCh chan<- FileChunk) error {
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
				row := make([]any, len(fields))
				listAccum := make(map[int][]string)

				for _, val := range prow {
					colIdx := val.Column()
					field := fields[colIdx]

					if val.IsNull() {
						row[colIdx] = nil
						continue
					}

					if field.Type().LogicalType() != nil && field.Type().LogicalType().List != nil {
						listAccum[colIdx] = append(listAccum[colIdx], val.String())
						continue
					}

					if field.Type().LogicalType() != nil {
						if field.Type().LogicalType().Timestamp != nil {
							ts := field.Type().LogicalType().Timestamp
							if ts.Unit.Millis != nil {
								row[colIdx] = time.UnixMilli(val.Int64()).UTC()
							} else if ts.Unit.Micros != nil {
								million := int64(math.Pow10(6))
								row[colIdx] = time.Unix(val.Int64()/million, (val.Int64()%million)*1_000).UTC()
							}

							continue
						}

						if field.Type().LogicalType().Date != nil {
							row[colIdx] = time.Unix(0, 0).UTC().AddDate(0, 0, int(val.Int32()))
							continue
						}
					}

					row[colIdx] = val.String()
				}

				for colIdx, elems := range listAccum {
					row[colIdx] = elems
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
