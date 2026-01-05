package parser

import (
	"schemift/core"
	"schemift/parser/mysql"
)

type SQLParser struct {
	mysqlParser *mysql.Parser
}

func NewSQLParser() *SQLParser {
	return &SQLParser{
		mysqlParser: mysql.NewParser(),
	}
}

func (p *SQLParser) ParseSchema(sql string) (*core.Database, error) {
	return p.mysqlParser.Parse(sql)
}
