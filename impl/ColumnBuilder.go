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
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/ipc"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"golang.org/x/exp/maps"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
	"golang.org/x/xerrors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FixedField struct {
	Len         int
	DestinField arrow.Field
	SourceType  arrow.DataType
	TableId     int
}

type FixedRow struct {
	FixedField []FixedField
}

type FixedSizeTableChunk struct {
	Chunkr         int
	FixedSizeTable *FixedSizeTable
	ColumnBuilders []ColumnBuilder
	RecordBuilder  []*array.RecordBuilder
	Record         []arrow.Record
	Bytes          []byte

	LinesParsed       int
	DurationReadChunk time.Duration
	DurationToArrow   time.Duration
	DurationToExport  time.Duration
}

type FixedSizeTable struct {
	// pointer to bytebuffer
	Bytes                []byte
	TableChunks          []FixedSizeTableChunk
	Row                  *FixedRow
	mem                  *memory.GoAllocator
	Schema               []arrow.Schema
	wg                   *sync.WaitGroup
	Records              [][]arrow.Record
	TableColAmount       []int
	Header               string
	Footer               string
	HasHeader            bool
	HasFooter            bool
	CalcHash             bool
	SourceEncoding       string
	ConsumeLineFunc      func(line string, fstc *FixedSizeTableChunk)
	FindLastNL           func(bytes []byte) int
	CustomParams         interface{}
	CustomColumnBuilders map[arrow.Type]func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder

	Cores              int
	LinesParsed        int
	Hash               []byte
	DurationReadChunk  time.Duration
	DurationToArrow    time.Duration
	DurationToExport   time.Duration
	DurationDoneExport time.Duration
	ColumnsizeCap      int
}

//const columnsizeCap = 3000000

var ColumnBuilders map[arrow.Type]func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder

func init() {

	ColumnBuilders = map[arrow.Type]func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder{
		arrow.BinaryTypes.String.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderString{fixedField: fixedField, recordBuilder: builder, values: make([]string, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Date32.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderDate32{fixedField: fixedField, recordBuilder: builder, values: make([]arrow.Date32, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Date64.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderDate64{fixedField: fixedField, recordBuilder: builder, values: make([]arrow.Date64, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Int8.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderInt8{fixedField: fixedField, recordBuilder: builder, values: make([]int8, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Int16.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderInt16{fixedField: fixedField, recordBuilder: builder, values: make([]int16, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Int32.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderInt32{fixedField: fixedField, recordBuilder: builder, values: make([]int32, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Int64.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderInt64{fixedField: fixedField, recordBuilder: builder, values: make([]int64, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Uint8.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderUint8{fixedField: fixedField, recordBuilder: builder, values: make([]uint8, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Uint16.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderUint16{fixedField: fixedField, recordBuilder: builder, values: make([]uint16, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Uint32.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderUint32{fixedField: fixedField, recordBuilder: builder, values: make([]uint32, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Uint64.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderUint64{fixedField: fixedField, recordBuilder: builder, values: make([]uint64, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Float32.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderFloat32{fixedField: fixedField, recordBuilder: builder, values: make([]float32, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.PrimitiveTypes.Float64.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
			var result ColumnBuilder
			result = &ColumnBuilderFloat64{fixedField: fixedField, recordBuilder: builder, values: make([]float64, 0, columnsizeCap), valid: make([]bool, 0, columnsizeCap), fieldnr: fieldNr}
			return &result
		},
		arrow.FixedWidthTypes.Boolean.ID(): func(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
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

	f.RecordBuilder = make([]*array.RecordBuilder, len(f.FixedSizeTable.Schema))

	for i := 0; i < len(f.FixedSizeTable.TableColAmount); i++ {
		f.RecordBuilder[i] = array.NewRecordBuilder(f.FixedSizeTable.mem, &f.FixedSizeTable.Schema[i])
	}
	tableIndex := -1
	tca := 0

	var fieldNr int
	for i, ff := range f.FixedSizeTable.Row.FixedField {
		if 0 == tca {
			tableIndex++
			tca = f.FixedSizeTable.TableColAmount[tableIndex]
			fieldNr = 0
		}
		f.ColumnBuilders[i] = *CreateColumBuilder(&ff, f.RecordBuilder[tableIndex], ff.Len, fieldNr, f.FixedSizeTable.ColumnsizeCap)
		tca--
		fieldNr++
	}
	return true
}

func SaveFeather(w *os.File, fst *FixedSizeTable) error {
	mem := memory.NewGoAllocator()

	tbl := array.NewTableFromRecords(&fst.Schema[0], fst.Records[0])
	rr := array.NewTableReader(tbl, 1010000)

	ww, err := ipc.NewFileWriter(w, ipc.WithAllocator(mem), ipc.WithSchema(rr.Schema()))
	if err != nil {
		return xerrors.Errorf("could not create ARROW file writer: %w", err)
	}

	defer ww.Close()

	return nil
}

// Read chunks of file and process them in go route after each chunk read. Slow disk is non non zero disk like sans etc

func CreateFixedSizeTableFromFile(fst *FixedSizeTable, row *FixedRow, reader *io.Reader, size int64) error {

	if nil == fst.FindLastNL && strings.ToLower(fst.SourceEncoding) == "utf-8" {
		fst.FindLastNL = FindLastNL_NO_CR
		fmt.Println("NO CR for eol")
	} else if nil == fst.FindLastNL {
		fmt.Println("NLCR for eol")
		fst.FindLastNL = FindLastNLCR
	}

	if nil == fst.TableColAmount {
		fst.TableColAmount = []int{len(row.FixedField)}
	}

	if nil == fst.ConsumeLineFunc {
		fst.ConsumeLineFunc = ConsumeLine
	}

	if nil != fst.CustomColumnBuilders {
		maps.Copy(ColumnBuilders, fst.CustomColumnBuilders)
	}

	if size < 20480 { // No multicore for silly small files.
		fst.Cores = 1
	}

	fst.Row = row
	fst.mem = memory.NewGoAllocator()
	fst.Schema = createSchemaFromFixedRow(*fst)

	fst.wg = &sync.WaitGroup{}

	err := ParalizeChunks(fst, reader, size)
	if nil != err {
		return err
	}

	return nil
}

func createSchemaFromFixedRow(fst FixedSizeTable) []arrow.Schema {
	var pos int
	var res []arrow.Schema

	res = make([]arrow.Schema, len(fst.TableColAmount))

	for i, len := range fst.TableColAmount {

		var fields []arrow.Field
		fields = make([]arrow.Field, len)

		for index, element := range fst.Row.FixedField[pos : pos+len] {
			fields[index] = element.DestinField
		}
		pos += len
		res[i] = *arrow.NewSchema(fields, nil)
	}
	return res
}

func FindLastNLCR(bytes []byte) int {
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

func FindLastNL_NO_CR(bytes []byte) int {
	p2 := len(bytes)
	if 0 == p2 {
		fmt.Println("FindLastNL_NO_CR got empty byte buffer!!!")
		return -1
	}

	for p2 > 0 {
		if p2 < len(bytes) && bytes[p2] == 0x0a {
			return p2 
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

func CreateColumBuilder(fixedField *FixedField, builder *array.RecordBuilder, columnsize int, fieldNr int, columnsizeCap int) *ColumnBuilder {
	return ColumnBuilders[fixedField.SourceType.ID()](fixedField, builder, columnsize, fieldNr, columnsizeCap)
}

func ReleaseRecordsForSchema(fst *FixedSizeTable, i int) {

	for j := 0; j < len(fst.Records[i]); j++ {
		fst.Records[i][j].Release()
	}

}

func ReleaseRecordBuilders(fst *FixedSizeTable) {
	for k := 0; k < len(fst.TableChunks); k++ {
		for j := 0; j < len(fst.TableChunks[k].RecordBuilder); j++ {
			fst.TableChunks[k].RecordBuilder[j].Release()
		}
	}
}
func ParalizeChunks(fst *FixedSizeTable, reader *io.Reader, size int64) error {
	fst.Bytes = make([]byte, size)
	fst.TableChunks = make([]FixedSizeTableChunk, fst.Cores)

	chunkSize := size / int64(fst.Cores)

	goon := true
	chunkNr := 0
	p1 := 0
	p2 := 0
	sha := sha256.New()
	for goon {
		var headerChunk, footerChunk bool

		fst.TableChunks[chunkNr] = FixedSizeTableChunk{FixedSizeTable: fst, Chunkr: chunkNr}
		fst.TableChunks[chunkNr].createColumBuilders()

		i1 := int(chunkSize) * chunkNr
		i2 := int(chunkSize) * (chunkNr + 1)
		if chunkNr == (fst.Cores - 1) {
			i2 = len(fst.Bytes)
		}
		buf := fst.Bytes[i1:i2]
		startReadChunk := time.Now()
		nread, err := io.ReadFull(*reader, buf)
		if nil != err {
			return err
		}

		fst.TableChunks[chunkNr].DurationReadChunk = time.Since(startReadChunk)
		buf = buf[:nread]

		if fst.CalcHash {
			sha.Write(buf)
		}

		goon = i2 < len(fst.Bytes)
		i_last_nl := fst.FindLastNL(buf)
		if i_last_nl == -1 {
			return errors.New("no data..check config")
		}
		p2 = i1 + i_last_nl
		fst.TableChunks[chunkNr].Bytes = fst.Bytes[p1:p2]
		p1 = p2

		if 0 == chunkNr && fst.HasHeader {
			headerChunk = true
		}

		if fst.HasFooter && !goon {
			footerChunk = true
		}

		fst.wg.Add(1)
		go fst.TableChunks[chunkNr].process(headerChunk, footerChunk)

		chunkNr++
	}
	fst.wg.Wait()

	//	var r []array.Record=make([]array.Record, len(fst.TableChunks))
	fst.Records = make([][]arrow.Record, len(fst.TableColAmount))

	for i := 0; i < len(fst.TableColAmount); i++ {
		fst.Records[i] = make([]arrow.Record, chunkNr)
		for j := 0; j < len(fst.TableChunks); j++ {
			fst.Records[i][j] = fst.TableChunks[j].Record[i] // TODO
		}
	}

	// Sum up some statitics
	for _, tableChunk := range fst.TableChunks {
		fst.DurationToArrow += tableChunk.DurationToArrow
		fst.DurationReadChunk += tableChunk.DurationReadChunk
		fst.DurationToExport += tableChunk.DurationToExport
		fst.LinesParsed += tableChunk.LinesParsed
	}

	// Finalize the SHA checksum
	if fst.CalcHash {
		fst.Hash = sha.Sum(nil)
	}
	return nil
}

func (fstc *FixedSizeTableChunk) process(lfHeader bool, lfFooter bool) int {
	startToArrow := time.Now()
	defer fstc.FixedSizeTable.wg.Done()

	var bbb []byte

	if lfFooter {
		p := fstc.FixedSizeTable.FindLastNL(fstc.Bytes)
		fstc.FixedSizeTable.Footer = string(fstc.Bytes[p:])
		bbb = fstc.Bytes[0:p]
	} else {
		bbb = fstc.Bytes
	}

	re := bytes.NewReader(bbb)
	var scanner bufio.Scanner

	switch strings.ToLower(fstc.FixedSizeTable.SourceEncoding) {
	case "iso8859-1":
		scanner = *bufio.NewScanner(transform.NewReader(re, charmap.ISO8859_1.NewDecoder()))

	case "utf-8":
		scanner = *bufio.NewScanner(re)
	default:
		scanner = *bufio.NewScanner(re)
	}
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

	//#	fstc.Record[0] = fstc.RecordBuilder[0].NewRecord()
	for _, rb := range fstc.RecordBuilder {
		fstc.Record = append(fstc.Record, rb.NewRecord())
	}

	//	fstc.Record=append(fstc.Record,)
	//	fstc.Record[0] = fstc.RecordBuilder[0].NewRecord()

	fstc.LinesParsed = lineCnt
	fstc.DurationToArrow = time.Since(startToArrow)
	return lineCnt
}

// TODO fix for utf8 !!! this is only for Ascii/8851-9 one byte coded glyphs
func ConsumeLine(line string, fstc *FixedSizeTableChunk) {
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

	ti = time.Date(int(year64), time.Month(month64), int(day64), int(hour64), int(minute64), int(second64), 0, time.UTC)

	return nil, ti.Unix()

}

// 2000-05-13-09.00.00.000000

func DateStringT1ToUnixMicro(dateString string) (error, time.Time) {

	var year64, month64, day64, hour64, minute64, second64, micro64, nano64 int64
	var err error

	year64, err = strconv.ParseInt(dateString[:4], 10, 32)

	if nil != err {
		return err, time.Time{}
	}

	month64, err = strconv.ParseInt(dateString[5:7], 10, 8)

	if nil != err {
		return err, time.Time{}
	}

	day64, err = strconv.ParseInt(dateString[8:10], 10, 8)
	if nil != err {
		return err, time.Time{}
	}

	hour64, err = strconv.ParseInt(dateString[11:13], 10, 8)
	if nil != err {
		return err, time.Time{}
	}

	minute64, err = strconv.ParseInt(dateString[14:16], 10, 8)
	if nil != err {
		return err, time.Time{}
	}

	second64, err = strconv.ParseInt(dateString[17:19], 10, 8)
	if nil != err {
		return err, time.Time{}
	}

	micro64, err = strconv.ParseInt(dateString[20:26], 10, 24)
	if nil != err {
		return err, time.Time{}
	}

	nano64 = micro64 * 1000
	var ti time.Time

	ti = time.Date(int(year64), time.Month(month64), int(day64), int(hour64), int(minute64), int(second64), int(nano64), time.UTC)

	return nil, ti

}

// 2000-05-13-09.00.00.000000000
func DateStringT1ToUnixNano(dateString string) (error, time.Time) {

	var year64, month64, day64, hour64, minute64, second64, nano64 int64
	var err error

	year64, err = strconv.ParseInt(dateString[:4], 10, 32)

	if nil != err {
		return err, time.Time{}
	}

	month64, err = strconv.ParseInt(dateString[5:7], 10, 8)

	if nil != err {
		return err, time.Time{}
	}

	day64, err = strconv.ParseInt(dateString[8:10], 10, 8)
	if nil != err {
		return err, time.Time{}
	}

	hour64, err = strconv.ParseInt(dateString[11:13], 10, 8)
	if nil != err {
		return err, time.Time{}
	}

	minute64, err = strconv.ParseInt(dateString[14:16], 10, 8)
	if nil != err {
		return err, time.Time{}
	}

	second64, err = strconv.ParseInt(dateString[17:19], 10, 8)
	if nil != err {
		return err, time.Time{}
	}

	nano64, err = strconv.ParseInt(dateString[20:29], 10, 24)
	if nil != err {
		return err, time.Time{}
	}

	var ti time.Time

	ti = time.Date(int(year64), time.Month(month64), int(day64), int(hour64), int(minute64), int(second64), int(nano64), time.UTC)

	return nil, ti

}

func IsError(err error) bool {
	if err != nil {
		fmt.Println(err.Error())
	}
	return (err != nil)
}
