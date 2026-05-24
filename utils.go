package main

import (
	"errors"
	"fmt"
	"io"
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
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %s", err)
	}

	pf, err := parquet.OpenFile(
		file, stat.Size(),
		parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true),
	)
	if err != nil {
		return fmt.Errorf("failed to read file: %s", err)
	}

	for _, rg := range pf.RowGroups() {
		reader := parquet.NewGenericRowGroupReader[any](rg)

		buf := make([]parquet.Row, chunkSize)
		for i := range buf {
			buf[i] = make(parquet.Row, 0, len(fields))
		}

		for {
			for i := range buf {
				buf[i] = buf[i][:0]
			}

			n, err := reader.ReadRows(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return fmt.Errorf("failed to read rows: %s", err)
			}

			if n > 0 {
				chunk := make([][]any, n)
				listAccum := make(map[int][]string)

				for i, prow := range buf[:n] {
					clear(listAccum)
					row := make([]any, len(fields))

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
									million := int64(1_000_000)
									row[colIdx] = time.Unix(val.Int64()/million, (val.Int64()%million)*1_000).UTC()
								}

								continue
							}

							if field.Type().LogicalType().Date != nil {
								row[colIdx] = time.Unix(0, 0).UTC().AddDate(0, 0, int(val.Int32()))
								continue
							}
						}

						switch {
						case field.Type().Kind() == parquet.Int64:
							row[colIdx] = val.Int64()
						case field.Type().Kind() == parquet.Int32:
							row[colIdx] = val.Int32()
						case field.Type().Kind() == parquet.Float:
							row[colIdx] = val.Float()
						case field.Type().Kind() == parquet.Double:
							row[colIdx] = val.Double()
						case field.Type().Kind() == parquet.ByteArray:
							row[colIdx] = val.String()
						default:
							row[colIdx] = val.String()
						}
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

		reader.Close()
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
