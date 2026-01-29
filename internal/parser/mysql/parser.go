// Package mysql inside parser, provides implementation to parse MySQL schema dumps.
// It uses TiDB's parser, so we support both MySQL syntax and TiDB-specific options.
package mysql

import (
	"fmt"
	"strings"

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

	if err := validateDatabase(db); err != nil {
		return nil, err
	}

	return db, nil
}

func validateDatabase(db *core.Database) error {
	if db == nil {
		return fmt.Errorf("invalid schema: database is nil")
	}

	for i, table := range db.Tables {
		if table == nil {
			return fmt.Errorf("invalid schema: table at index %d is nil", i)
		}
		if strings.TrimSpace(table.Name) == "" {
			return fmt.Errorf("invalid schema: table at index %d has empty name", i)
		}
		if len(table.Columns) == 0 {
			return fmt.Errorf("invalid schema: table %q has no columns", table.Name)
		}
		for j, col := range table.Columns {
			if col == nil {
				return fmt.Errorf("invalid schema: column at index %d in table %q is nil", j, table.Name)
			}
			if strings.TrimSpace(col.Name) == "" {
				return fmt.Errorf("invalid schema: column at index %d in table %q has empty name", j, table.Name)
			}
		}
	}

	return nil
}

func (p *Parser) convertCreateTable(stmt *ast.CreateTableStmt) (*core.Table, error) {
	table := &core.Table{
		Name:    stmt.Table.Name.O,
		Columns: []*core.Column{},
	}

	p.parseTableOptions(stmt.Options, table)
	p.parseColumns(stmt.Cols, table)
	p.parseConstraints(stmt.Constraints, table)

	return table, nil
}
