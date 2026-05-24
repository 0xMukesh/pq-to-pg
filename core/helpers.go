package core

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

type TableMeta struct {
	Name   string
	Cols   []string
	Fields []parquet.Field
}

type FileChunk struct {
	TableName string
	Data      [][]any
}

func CollectFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read dir: %s", err)
	}

	allowedExts := []string{".pq", ".parquet"}
	var files []string

	for _, entry := range entries {
		if entry.IsDir() || !slices.Contains(allowedExts, filepath.Ext(entry.Name())) {
			continue
		}

		files = append(files, filepath.Join(dir, entry.Name()))
	}

	slices.SortFunc(files, func(a, b string) int {
		sa, _ := os.Stat(a)
		sb, _ := os.Stat(b)

		switch {
		case sa.Size() > sb.Size():
			return 1
		case sa.Size() < sb.Size():
			return -1
		default:
			return 0
		}
	})

	return files, nil
}

func BuildTableMetas(files []string) (map[string]TableMeta, error) {
	meta := make(map[string]TableMeta, len(files))

	for _, f := range files {
		schema, err := inferSchema(f)
		if err != nil {
			return nil, fmt.Errorf("failed to infer schema %s: %s", f, err)
		}

		cols := []string{}
		for _, col := range schema.Columns() {
			cols = append(cols, col[0])
		}

		base := filepath.Base(f)
		tableName := strings.TrimSuffix(base, filepath.Ext(base))

		meta[f] = TableMeta{
			Name:   tableName,
			Cols:   cols,
			Fields: schema.Fields(),
		}
	}

	return meta, nil
}

func readParquetFile(path string, chunksCh chan<- FileChunk, meta TableMeta, chunkSize int) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %s", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %s", err)
	}

	pf, err := parquet.OpenFile(
		f, stat.Size(),
		parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true),
	)
	if err != nil {
		return fmt.Errorf("failed to read file: %s", err)
	}

	for _, rg := range pf.RowGroups() {
		if err = readRowGroups(rg, meta, chunkSize, chunksCh); err != nil {
			return err
		}
	}

	return nil
}

func readRowGroups(rg parquet.RowGroup, meta TableMeta, chunkSize int, chunksCh chan<- FileChunk) error {
	reader := parquet.NewGenericRowGroupReader[any](rg)
	defer reader.Close()

	buf := make([]parquet.Row, chunkSize)
	for i := range buf {
		buf[i] = make(parquet.Row, 0, len(meta.Fields))
	}

	for {
		// clean up the buffer
		for i := range buf {
			buf[i] = buf[i][:0]
		}

		n, err := reader.ReadRows(buf)
		if err != nil {
			if errors.Is(io.EOF, err) {
				break
			}

			return fmt.Errorf("failed to read rows: %s", err)
		}

		if n > 0 {
			chunk := FileChunk{
				TableName: meta.Name,
				Data:      make([][]any, n),
			}
			// set up map for storing fields for colums which are lists
			listAccum := make(map[int][]string)

			for i, prow := range buf[:n] {
				clear(listAccum)
				row := make([]any, len(meta.Fields))

				for _, val := range prow {
					colIdx := val.Column()
					field := meta.Fields[colIdx]

					if val.IsNull() {
						row[colIdx] = nil // convert parquet null string to go nil value
						continue
					}

					// handle complex types like list, timestamp and date
					if field.Type().LogicalType() != nil {
						if field.Type().LogicalType().List != nil {
							listAccum[colIdx] = append(listAccum[colIdx], val.String())
							continue
						}

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

					// convert to native types
					switch field.Type().Kind() {
					case parquet.Int32:
						row[colIdx] = val.Int32()
					case parquet.Int64:
						row[colIdx] = val.Int64()
					case parquet.Float:
						row[colIdx] = val.Float()
					case parquet.Double:
						row[colIdx] = val.Double()
					case parquet.Boolean:
						row[colIdx] = val.Boolean()
					default:
						row[colIdx] = val.String()
					}
				}

				for colIdx, elems := range listAccum {
					row[colIdx] = elems
				}
				chunk.Data[i] = row
			}

			chunksCh <- chunk
		}
	}

	return nil
}

func inferSchema(path string) (*parquet.Schema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %s", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %s", err)
	}

	pf, err := parquet.OpenFile(f, stat.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, fmt.Errorf("failed to parse parquet: %s", err)
	}

	return pf.Schema(), nil
}
