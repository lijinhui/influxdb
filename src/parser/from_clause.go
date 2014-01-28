package parser

// #include "query_types.h"
// #include <stdlib.h>
import "C"
import "bytes"
import "fmt"

type FromClauseType int

const (
	FromClauseArray     FromClauseType = C.FROM_ARRAY
	FromClauseMerge     FromClauseType = C.FROM_MERGE
	FromClauseInnerJoin FromClauseType = C.FROM_INNER_JOIN
)

func (self *TableName) GetAlias() string {
	if self.Alias != "" {
		return self.Alias
	}
	return self.Name.Name
}

func (self *TableName) GetAliasString() string {
	if self.Alias != "" {
		return fmt.Sprintf("as %s", self.Alias)
	}
	return ""
}

type TableName struct {
	Name  *Value
	Alias string
}

type FromClause struct {
	Type  FromClauseType
	Names []*TableName
}

func (self *FromClause) GetString() string {
	buffer := bytes.NewBufferString("")
	switch self.Type {
	case FromClauseMerge:
		fmt.Fprintf(buffer, "%s%s merge %s %s", self.Names[0].Name.GetString(), self.Names[1].GetAliasString(),
			self.Names[1].Name.GetString(), self.Names[1].GetAliasString())
	case FromClauseInnerJoin:
		fmt.Fprintf(buffer, "%s%s inner join %s %s", self.Names[0].Name.GetString(), self.Names[1].GetAliasString(),
			self.Names[1].Name.GetString(), self.Names[1].GetAliasString())
	default:
		for idx, t := range self.Names {
			if idx != 0 {
				fmt.Fprintf(buffer, ", ")
			}
			alias := ""
			if t.Alias != "" {
				alias = fmt.Sprintf(" as %s", t.Alias)
			}
			fmt.Fprintf(buffer, "%s%s", t.Name.GetString(), alias)
		}
	}
	return buffer.String()
}
