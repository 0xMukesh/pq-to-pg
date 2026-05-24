package core

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ConnectToDb(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	dbCfg, err := pgxpool.ParseConfig(cfg.DbConn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse db conn url: %s", err)
	}
	dbCfg.MaxConns = int32(cfg.NumWriters)

	pool, err := pgxpool.NewWithConfig(ctx, dbCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to setup pool: %s", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %s", err)
	}

	return pool, nil
}

func CreateTables(ctx context.Context, pool *pgxpool.Pool, tableMetas map[string]TableMeta) error {
	batch := &pgx.Batch{}

	for _, meta := range tableMetas {
		sb := &strings.Builder{}
		fmt.Fprintf(sb, "CREATE TABLE IF NOT EXISTS %s (", meta.Name)

		for i, field := range meta.Fields {
			pgType, ok := pqToPgMapping[field.Type().String()]
			if !ok {
				lt := field.Type().LogicalType()

				if lt != nil && lt.List != nil {
					elemType := field.Fields()[0].Fields()[0].Type().String()
					pgType, ok = pqToPgMapping[elemType]
					if !ok {
						return fmt.Errorf("unsupported list element type: %s", elemType)
					}

					pgType += "[]"
				} else {
					return fmt.Errorf("unsupported parquet type: %s", field)
				}
			}

			fmt.Fprintf(sb, `"%s" %s`, field.Name(), pgType)
			if i < len(meta.Fields)-1 {
				sb.WriteString(", ")
			}
		}

		sb.WriteString(");")
		batch.Queue(sb.String())
	}

	br := pool.SendBatch(ctx, batch)
	defer br.Close()

	_, err := br.Exec()
	return err
}
