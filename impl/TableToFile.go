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
	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/parquet"
	"github.com/apache/arrow/go/v7/parquet/compress"
	"github.com/apache/arrow/go/v7/parquet/pqarrow"
	"io"
	"time"
)

func SaveToParquet(fst *FixedSizeTable, writer io.Writer) error {
	startWaitDoneExport := time.Now()

	tbl := array.NewTableFromRecords(&fst.Schema[0], fst.Records[0])

	i := int64(len(fst.TableChunks[0].Bytes))

	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithCompression(compress.Codecs.Snappy))
	arrProps := pqarrow.DefaultWriterProps()

	err := pqarrow.WriteTable(tbl, writer, i, props, arrProps)
	fst.DurationDoneExport = time.Since(startWaitDoneExport)
	return err
}

func saveToFeather(sc *arrow.Schema, table *array.TableReader, w io.Writer) {

}
