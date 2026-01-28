// Package mysql inside parser, provides implementation to parse MySQL schema dumps.
// It uses TiDB's parser, so we support both MySQL syntax and TiDB-specific options.
package mysql

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"smf/internal/core"
)

type Parser struct {
	p *parser.Parser
}

func NewParser() *Parser {
	return &Parser{p: parser.New()}
}

func (p *Parser) Parse(sql string) (*core.Database, error) {
	// TODO: add support to specify charset and collation
	// NOTE: this can be parallelized, it can help if schema dumps are big.
	stmtNodes, _, err := p.p.Parse(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	db := &core.Database{Tables: []*core.Table{}}
	for _, stmt := range stmtNodes {
		if create, ok := stmt.(*ast.CreateTableStmt); ok {
			table, err := p.convertCreateTable(create)
			if err != nil {
				return nil, err
			}
			db.Tables = append(db.Tables, table)
		}
	}

	return db, nil
}

func (p *Parser) convertCreateTable(stmt *ast.CreateTableStmt) (*core.Table, error) {
	table := &core.Table{
		Name:    stmt.Table.Name.O,
		Columns: []*core.Column{},
	}

	p.parseColumns(stmt.Cols, table)
	p.parseConstraints(stmt.Constraints, table)

	return table, nil
}
