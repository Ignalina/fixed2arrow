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
	"github.com/apache/arrow/go/v10/arrow/array"
)

type ColumnBuilderBoolean struct {
	fixedField    *FixedField
	recordBuilder *array.RecordBuilder
	fieldnr       int
	values        []bool
	valid         []bool
}

// make configurable
func (c *ColumnBuilderBoolean) ParseValue(name string) bool {
	boolChar := name[0]
	var ourBool bool

	switch boolChar {
	case uint8('J'):
		ourBool = true
		break

	case uint8('j'):
		ourBool = true
		break

	case uint8('Y'):
		ourBool = true
		break
	case uint8('y'):
		ourBool = true
		break

	case uint8('N'):
		ourBool = false
		break

	case uint8('n'):
		ourBool = false
		break
	default:
		c.values = append(c.values, false)
		c.valid = append(c.valid, false)
		return false
	}

	c.values = append(c.values, ourBool)
	c.valid = append(c.valid, true)
	return true
}

func (c *ColumnBuilderBoolean) FinishColumn() bool {
	c.recordBuilder.Field(c.fieldnr).(*array.BooleanBuilder).AppendValues(c.values, c.valid)
	return true
}

func (c *ColumnBuilderBoolean) Nullify() {
	c.values = append(c.values, false)
	c.valid = append(c.valid, false)
}
