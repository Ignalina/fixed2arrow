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
	"bufio"
	"bytes"
	"fmt"
	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/ipc"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"golang.org/x/exp/maps"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
	"golang.org/x/xerrors"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

type FixedField struct {
	Len         int
	DestinField arrow.Field
	SourceType  arrow.DataType
}

type FixedRow struct {
	FixedField []FixedField
}

type FixedSizeTableChunk struct {
	FixedSizeTable *FixedSizeTable
	ColumnBuilders []ColumnBuilder
	RecordBuilder  *array.RecordBuilder
	Record         arrow.Record
	Bytes          []byte
}

type FixedSizeTable struct {
	// pointer to bytebuffer
	Bytes                []byte
	TableChunks          []FixedSizeTableChunk
	Row                  *FixedRow
	mem                  *memory.GoAllocator
	Schema               *arrow.Schema
	wg                   *sync.WaitGroup
	Records              []arrow.Record
	Header               string
	Footer               string
	HasHeader            bool
	HasFooter            bool
	ConsumeLineFunc      func(line string, fstc FixedSizeTableChunk)
	CustomParams         interface{}
	CustomColumnBuilders map[arrow.Type]func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder
}

const columnsizeCap = 3000000

var ColumnBuilders map[arrow.Type]func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder

func init() {

	ColumnBuilders = map[arrow.Type]func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder{
		arrow.BinaryTypes.String.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderString{fixedField: fixedField, recordBuilder: builder, values: make([]string, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Date32.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderDate32{fixedField: fixedField, recordBuilder: builder, values: make([]arrow.Date32, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Date64.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderDate64{fixedField: fixedField, recordBuilder: builder, values: make([]arrow.Date64, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Int8.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderInt8{fixedField: fixedField, recordBuilder: builder, values: make([]int8, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Int16.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderInt16{fixedField: fixedField, recordBuilder: builder, values: make([]int16, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Int32.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderInt32{fixedField: fixedField, recordBuilder: builder, values: make([]int32, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Int64.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderInt64{fixedField: fixedField, recordBuilder: builder, values: make([]int64, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Uint8.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderUint8{fixedField: fixedField, recordBuilder: builder, values: make([]uint8, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Uint16.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderUint16{fixedField: fixedField, recordBuilder: builder, values: make([]uint16, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Uint32.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderUint32{fixedField: fixedField, recordBuilder: builder, values: make([]uint32, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Uint64.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderUint64{fixedField: fixedField, recordBuilder: builder, values: make([]uint64, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Float32.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderFloat32{fixedField: fixedField, recordBuilder: builder, values: make([]float32, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Float64.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderFloat64{fixedField: fixedField, recordBuilder: builder, values: make([]float64, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.FixedWidthTypes.Boolean.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderBoolean{fixedField: fixedField, recordBuilder: builder, values: make([]bool, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
	}
}

func (f FixedRow) CalRowLength() int {
	sum := 0

	for _, num := range f.FixedField {
		sum += num.Len
	}
	return sum + 2
}

func (f *FixedSizeTableChunk) createColumBuilders() bool {
	f.ColumnBuilders = make([]ColumnBuilder, len(f.FixedSizeTable.Row.FixedField))

	f.RecordBuilder = array.NewRecordBuilder(f.FixedSizeTable.mem, f.FixedSizeTable.Schema)
	//	defer b.Release()

	for i, ff := range f.FixedSizeTable.Row.FixedField {
		f.ColumnBuilders[i] = *CreateColumBuilder(&ff, f.RecordBuilder, ff.Len, i)
	}
	return true
}

func SaveFeather(w *os.File, fst *FixedSizeTable) error {
	mem := memory.NewGoAllocator()

	tbl := array.NewTableFromRecords(fst.Schema, fst.Records)
	rr := array.NewTableReader(tbl, 1010000)

	ww, err := ipc.NewFileWriter(w, ipc.WithAllocator(mem), ipc.WithSchema(rr.Schema()))
	if err != nil {
		return xerrors.Errorf("could not create ARROW file writer: %w", err)
	}

	defer ww.Close()

	return nil
}

// Read chunks of file and process them in go route after each chunk read. Slow disk is non non zero disk like sans etc

func CreateFixedSizeTableFromFile(fst *FixedSizeTable, row *FixedRow, reader *io.Reader, size int64, cores int) error {
	if nil == fst.ConsumeLineFunc {
		fst.ConsumeLineFunc = ConsumeLine
	}

	if nil != fst.CustomColumnBuilders {
		maps.Copy(ColumnBuilders, fst.CustomColumnBuilders)
	}

	fst.Row = row
	fst.mem = memory.NewGoAllocator()
	fst.Schema = createSchemaFromFixedRow(row)

	fst.wg = &sync.WaitGroup{}

	ParalizeChunks(fst, reader, size, cores)

	//	defer tbl.Release()

	return nil
}

func createSchemaFromFixedRow(row *FixedRow) *arrow.Schema {
	var fields []arrow.Field
	fields = make([]arrow.Field, len(row.FixedField))

	for index, element := range row.FixedField {
		fields[index] = element.DestinField
	}
	return arrow.NewSchema(fields, nil)
}

//  unsigned char glyph=(unsigned char)195;
func findLastNL(bytes []byte) int {
	p2 := len(bytes)
	if 0 == p2 {
		return -1
	}

	for p2 > 0 {
		if p2 < len(bytes) && bytes[p2-1] == 0x0d && bytes[p2] == 0x0a {
			return p2 + 1
		}
		p2--
	}

	return 0
}

type ColumnBuilder interface {
	ParseValue(name string) bool
	FinishColumn() bool
	Nullify()
}

func CreateColumBuilder(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int) *ColumnBuilder {
	return ColumnBuilders[fixedField.SourceType.ID()](fixedField, builder, columnsize, fieldNr)
}

func ParalizeChunks(fst *FixedSizeTable, reader *io.Reader, size int64, core int) error {

	fst.Bytes = make([]byte, size)
	fst.TableChunks = make([]FixedSizeTableChunk, core)

	chunkSize := size / int64(core)
	rowlength := int64(fst.Row.CalRowLength())

	if chunkSize < int64(rowlength) {
		chunkSize = int64(rowlength)
	}

	goon := true
	chunkNr := 0
	p1 := 0
	p2 := 0

	for goon {
		var headerChunk, footerChunk bool

		fst.TableChunks[chunkNr] = FixedSizeTableChunk{FixedSizeTable: fst}
		fst.TableChunks[chunkNr].createColumBuilders()

		i1 := int(chunkSize) * chunkNr
		i2 := int(chunkSize) * (chunkNr + 1)
		if chunkNr == (core - 1) {
			i2 = len(fst.Bytes)
		}
		buf := fst.Bytes[i1:i2]
		nread, _ := io.ReadFull(*reader, buf)
		buf = buf[:nread]
		goon = i2 < len(fst.Bytes)
		p2 = i1 + findLastNL(buf)
		fst.TableChunks[chunkNr].Bytes = fst.Bytes[p1:p2]
		p1 = p2

		if 0 == chunkNr && fst.HasHeader {
			headerChunk = true
		}

		if fst.HasFooter && !goon {
			footerChunk = true
		}

		fst.wg.Add(1)
		fst.TableChunks[chunkNr].process(headerChunk, footerChunk)
		fst.TableChunks[chunkNr].Record = fst.TableChunks[chunkNr].RecordBuilder.NewRecord()

		chunkNr++
	}
	fst.wg.Wait()

	//	var r []array.Record=make([]array.Record, len(fst.TableChunks))
	fst.Records = make([]arrow.Record, len(fst.TableChunks))

	for i, num := range fst.TableChunks {
		fst.Records[i] = num.Record
	}

	return nil
}

func (fstc FixedSizeTableChunk) process(lfHeader bool, lfFooter bool) int {

	defer fstc.FixedSizeTable.wg.Done()

	var bbb []byte

	if lfFooter {
		p := findLastNL(fstc.Bytes)
		bbb = fstc.Bytes[0:p]
		fstc.FixedSizeTable.Footer = string(fstc.Bytes[p:])
	} else {
		bbb = fstc.Bytes
	}

	re := bytes.NewReader(bbb)
	decodingReader := transform.NewReader(re, charmap.ISO8859_1.NewDecoder()) //   lines := []string{}

	scanner := bufio.NewScanner(decodingReader)
	lineCnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		lineCnt++

		if lfHeader && 1 == lineCnt {
			fstc.FixedSizeTable.Header = line
			continue
		}

		fstc.FixedSizeTable.ConsumeLineFunc(line, fstc)
		//		fstc.consumeLine(line)

	}
	// TODO check scanner.err()
	for ci, _ := range fstc.FixedSizeTable.Row.FixedField {
		fstc.ColumnBuilders[ci].FinishColumn()
	}

	if lfHeader {
		lineCnt--
	}

	if lfFooter {
		lineCnt--
	}

	return lineCnt
}

func ConsumeLine(line string, fstc FixedSizeTableChunk) {
	var columnPos int
	for ci, cc := range fstc.FixedSizeTable.Row.FixedField {
		columString := line[columnPos : columnPos+cc.Len]
		fstc.ColumnBuilders[ci].ParseValue(columString)
		columnPos += cc.Len
	}
}

var lo = &time.Location{}

// 2020-07-09-09.59.59.99375
func DateStringT1ToUnix(dateString string) (error, int64) {

	var year64, month64, day64, hour64, minute64, second64 int64
	var err error

	year64, err = strconv.ParseInt(dateString[:4], 10, 32)

	if nil != err {
		return err, 0
	}

	month64, err = strconv.ParseInt(dateString[5:7], 10, 8)

	if nil != err {
		return err, 0
	}

	day64, err = strconv.ParseInt(dateString[8:10], 10, 8)
	if nil != err {
		return err, 0
	}

	hour64, err = strconv.ParseInt(dateString[11:13], 10, 8)
	if nil != err {
		return err, 0
	}

	minute64, err = strconv.ParseInt(dateString[14:16], 10, 8)
	if nil != err {
		return err, 0
	}

	second64, err = strconv.ParseInt(dateString[17:19], 10, 8)
	if nil != err {
		return err, 0
	}

	var ti time.Time

	ti = time.Date(int(year64), time.Month(month64), int(day64), int(hour64), int(minute64), int(second64), 0, lo)

	return nil, ti.Unix()

}

func IsError(err error) bool {
	if err != nil {
		fmt.Println(err.Error())
	}
	return (err != nil)
}
