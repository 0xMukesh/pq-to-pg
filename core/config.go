package core

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	ParquetDir string
	DbConn     string
	NumReaders int
	NumWriters int
	ChunkSize  int
}

func (c Config) validate() error {
	switch {
	case c.ParquetDir == "":
		return errors.New("missing --parquet-dir flag")
	case c.DbConn == "":
		return errors.New("missing --db-conn flag")
	case c.NumReaders == -1:
		return errors.New("missing --num-readers flag")
	case c.NumWriters == -1:
		return errors.New("missing --num-writers flag")
	case c.ChunkSize == -1:
		return errors.New("missing --chunk-size flag")
	}

	stat, err := os.Stat(c.ParquetDir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%s dir does not exist", c.ParquetDir)
		}

		return fmt.Errorf("failed to read %s dir: %s", c.ParquetDir, err)
	}
	if !stat.IsDir() {
		return fmt.Errorf("%s is not a dir", c.ParquetDir)
	}

	if _, err := pgxpool.ParseConfig(c.DbConn); err != nil {
		return fmt.Errorf("failed to parse db conn url: %s", err)
	}

	return nil
}

func ParseConfig() (Config, error) {
	var cfg Config

	flag.StringVar(&cfg.ParquetDir, "parquet-dir", "", "path to the directory which contains the parquet files")
	flag.StringVar(&cfg.DbConn, "db-conn", "", "connection url to the postgres db")
	flag.IntVar(&cfg.NumReaders, "num-readers", -1, "number of concurrent reader workers to run")
	flag.IntVar(&cfg.NumWriters, "num-writers", -1, "number of concurrent writer workers to run")
	flag.IntVar(&cfg.ChunkSize, "chunk-size", -1, "size of the chunk of parquet file content which the workers read at once")
	flag.Parse()

	// if err := cfg.validate(); err != nil {
	// 	log.Fatalf("failed to parse config: %s", err)
	// }

	return cfg, cfg.validate()
}
