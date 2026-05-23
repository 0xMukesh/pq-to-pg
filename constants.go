package main

import (
	"github.com/parquet-go/parquet-go"
)

// TODO: need to expand to much larger set
var pqToPgMapping = map[string]string{
	parquet.BooleanType.String():                                          "BOOLEAN",
	parquet.Int32Type.String():                                            "INTEGER",
	parquet.Int64Type.String():                                            "BIGINT",
	parquet.FloatType.String():                                            "REAL",
	parquet.DoubleType.String():                                           "DOUBLE PRECISION",
	parquet.String().Type().String():                                      "VARCHAR",
	parquet.UUID().Type().String():                                        "UUID",
	parquet.JSON().Type().String():                                        "JSONB",
	parquet.Date().Type().String():                                        "DATE",
	parquet.TimeAdjusted(parquet.Millisecond, true).Type().String():       "TIME WITH TIME ZONE",
	parquet.TimeAdjusted(parquet.Microsecond, true).Type().String():       "TIME WITH TIME ZONE",
	parquet.TimeAdjusted(parquet.Nanosecond, true).Type().String():        "TIME WITH TIME ZONE",
	parquet.TimeAdjusted(parquet.Millisecond, false).Type().String():      "TIME",
	parquet.TimeAdjusted(parquet.Microsecond, false).Type().String():      "TIME",
	parquet.TimeAdjusted(parquet.Nanosecond, false).Type().String():       "TIME",
	parquet.TimestampAdjusted(parquet.Millisecond, true).Type().String():  "TIMESTAMP WITH TIME ZONE",
	parquet.TimestampAdjusted(parquet.Microsecond, true).Type().String():  "TIMESTAMP WITH TIME ZONE",
	parquet.TimestampAdjusted(parquet.Nanosecond, true).Type().String():   "TIMESTAMP WITH TIME ZONE",
	parquet.TimestampAdjusted(parquet.Millisecond, false).Type().String(): "TIMESTAMP",
	parquet.TimestampAdjusted(parquet.Microsecond, false).Type().String(): "TIMESTAMP",
	parquet.TimestampAdjusted(parquet.Nanosecond, false).Type().String():  "TIMESTAMP",
}
