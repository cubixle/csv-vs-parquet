package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func main() {
	t := flag.String("type", "csv", "")
	rows := flag.Int("amount", 1000, "")

	flag.Parse()

	slog.Info("Running", "type", *t, "rows", *rows)

	switch *t {
	case "csv":
		err := writeCSV(*rows)
		if err != nil {
			log.Fatal(err)
		}
	case "parquet":
		err := writeParquet(*rows)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func writeCSV(rows int) error {
	const filename = "output.csv"

	f, err := os.Create(filename)
	if err != nil {
		return err
	}

	for i := 0; i < rows; i++ {
		_, err := fmt.Fprintf(f, "%d,ice hockey,pizza", i)
		if err != nil {
			return err
		}
	}

	return nil
}

type Row struct {
	ID    int    `parquet:"name=id, type=INT32, encoding=PLAIN"`
	Sport string `parquet:"name=sport, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Food  string `parquet:"name=food, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func writeParquet(rows int) error {
	const filename = "output.parquet"

	fw, err := local.NewLocalFileWriter(filename)
	if err != nil {
		return fmt.Errorf("failed to NewLocalFileWriter %w", err)
	}

	// write
	pw, err := writer.NewParquetWriter(fw, new(Row), 4)
	if err != nil {
		return fmt.Errorf("failed to NewParquertWriter %w", err)
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for i := 0; i < rows; i++ {
		r := Row{
			ID:    i,
			Food:  "pizza",
			Sport: "ice hockey",
		}
		if err = pw.Write(r); err != nil {
			return fmt.Errorf("failed to Write %w", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to WriteStop %w", err)
	}

	fw.Close()

	return nil
}
