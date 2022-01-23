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
	"github.com/apache/arrow/go/v7/arrow/arrio"
	"github.com/apache/arrow/go/v7/arrow/ipc"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/apache/arrow/go/v7/parquet/file"
	"golang.org/x/xerrors"
	"io"
	"os"
)

func saveToParquet(sc *arrow.Schema, table *array.TableReader, w io.Writer) {

	//parquet.WithCompression(compress.Codecs.Snappy)

	pw := file.NewParquetWriter(w, nil)

	print(pw)
}

func saveToFeather(table *array.TableReader) {
}

func processStream(w *os.File, r io.Reader) error {
	mem := memory.NewGoAllocator()

	rr, err := ipc.NewReader(r, ipc.WithAllocator(mem))
	if err != nil {
		if xerrors.Is(err, io.EOF) {
			return nil
		}
		return err
	}

	ww, err := ipc.NewFileWriter(w, ipc.WithAllocator(mem), ipc.WithSchema(rr.Schema()))
	if err != nil {
		return xerrors.Errorf("could not create ARROW file writer: %w", err)
	}
	defer ww.Close()

	_, err = arrio.Copy(ww, rr)
	if err != nil {
		return xerrors.Errorf("could not copy ARROW stream: %w", err)
	}

	err = ww.Close()
	if err != nil {
		return xerrors.Errorf("could not close output ARROW file: %w", err)
	}

	return nil
}
