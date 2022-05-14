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

package main

import (
	"fmt"
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/ignalina/fixed2arrow/impl"
	"io"
	"os"

	"time"
)

/**
* This is simple 2 column example,in future versions &fixedRow will be constructed dynamically from a schema.
 */

func main() {
	start := time.Now()

	fullPath := "test.last10"

	fixedRow := createFixedRow()

	var reader *io.Reader
	file, err := os.Open(fullPath)
	fi, _ := file.Stat()

	if err != nil {
		return
	}
	defer file.Close()
	*reader = file

	var fst impl.FixedSizeTable
	fst.Cores = 8
	fst.ColumnsizeCap = 3000000 / fst.Cores
	err = impl.CreateFixedSizeTableFromFile(&fst, &fixedRow, reader, fi.Size(), fst.Cores)
	if nil != err {
		fmt.Println("BAD!!!")
	}

	var outFile *os.File
	outFile, err = os.OpenFile(fullPath+".parqet", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if impl.IsError(err) {
		return
	}

	defer outFile.Close()

	err = impl.SaveToParquet(nil, outFile, 0)

	elapsed := time.Since(start)
	fmt.Println("elapesed total=", elapsed)

}

// Example table with 2 columns ,this should constructed from a schema in follow versions
func createFixedRow() impl.FixedRow {
	result := impl.FixedRow{
		[]impl.FixedField{
			impl.FixedField{11, arrow.Field{Name: "idnr", Type: arrow.PrimitiveTypes.Int64}, arrow.PrimitiveTypes.Int64, 0},
			impl.FixedField{20, arrow.Field{Name: "description", Type: arrow.BinaryTypes.String}, arrow.BinaryTypes.String, 0},
		},
	}
	return result
}
