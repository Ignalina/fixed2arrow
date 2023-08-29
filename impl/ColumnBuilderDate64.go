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
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
)

type ColumnBuilderDate64 struct {
	fixedField    *FixedField
	recordBuilder *array.RecordBuilder
	fieldnr       int
	values        []arrow.Date64
	valid         []bool
}

func (c *ColumnBuilderDate64) ParseValue(name string) bool {

	err, ti := DateStringT1ToUnixMicro(name)
	if nil != err {
		c.Nullify()
		return false
	}
	c.values = append(c.values, arrow.Date64FromTime(ti))
	c.valid = append(c.valid, true)

	return true
}

func (c *ColumnBuilderDate64) FinishColumn() bool {
	c.recordBuilder.Field(c.fieldnr).(*array.Date64Builder).AppendValues(c.values, c.valid)
	return true
}

func (c *ColumnBuilderDate64) Nullify() {
	c.values = append(c.values, 0)
	c.valid = append(c.valid, false)
}
