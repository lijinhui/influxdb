package parser

// #include "query_types.h"
// #include <stdlib.h>
import "C"

import (
	"bytes"
	"fmt"
	"regexp"
)

type ValueType int

const (
	ValueRegex        ValueType = C.VALUE_REGEX
	ValueInt                    = C.VALUE_INT
	ValueFloat                  = C.VALUE_FLOAT
	ValueString                 = C.VALUE_STRING
	ValueTableName              = C.VALUE_TABLE_NAME
	ValueSimpleName             = C.VALUE_SIMPLE_NAME
	ValueDuration               = C.VALUE_DURATION
	ValueWildcard               = C.VALUE_WILDCARD
	ValueFunctionCall           = C.VALUE_FUNCTION_CALL
	ValueExpression             = C.VALUE_EXPRESSION
)

type Value struct {
	Name          string
	Type          ValueType
	Elems         []*Value
	compiledRegex *regexp.Regexp
}

func (self *Value) IsFunctionCall() bool {
	return self.Type == ValueFunctionCall
}

func (self *Value) GetCompiledRegex() (*regexp.Regexp, bool) {
	return self.compiledRegex, self.Type == ValueRegex
}

func (self *Value) GetString() string {
	buffer := bytes.NewBufferString("")
	switch self.Type {
	case ValueExpression:
		fmt.Fprintf(buffer, "%s %s %s", self.Elems[0].GetString(), self.Name, self.Elems[1].GetString())
	case ValueFunctionCall:
		fmt.Fprintf(buffer, "%s(", self.Name)
		for idx, v := range self.Elems {
			if idx != 0 {
				fmt.Fprintf(buffer, ", ")
			}
			fmt.Fprintf(buffer, v.GetString())
		}
		fmt.Fprintf(buffer, ")")
	case ValueString:
		fmt.Fprintf(buffer, "'%s'", self.Name)
	default:
		fmt.Fprintf(buffer, self.Name)
	}
	return buffer.String()
}
