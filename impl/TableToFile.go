/*
 * MIT No Attribution
 *
 * Copyright 2021 Rickard Lundin (rickard@ignalina.dk)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package impl

import (
	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/parquet"
	"github.com/apache/arrow/go/v9/parquet/compress"
	"github.com/apache/arrow/go/v9/parquet/pqarrow"
	"io"
)

func SaveToParquet(schema *arrow.Schema, record []arrow.Record, writer io.Writer, i int64) error {
	var err error

	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithCompression(compress.Codecs.Snappy))
	arrProps := pqarrow.DefaultWriterProps()
	tbl := array.NewTableFromRecords(schema, record)

	err = pqarrow.WriteTable(tbl, writer, i, props, arrProps)

	tbl.Release()

	return err
}

func saveToFeather(sc *arrow.Schema, table *array.TableReader, w io.Writer) {

}
