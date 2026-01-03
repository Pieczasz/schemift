package mysql

import (
	"fmt"
	"strings"

	"schemift/internal/core"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

type Parser struct {
	p *parser.Parser
}

func NewParser() *Parser {
	return &Parser{
		p: parser.New(),
	}
}

func (p *Parser) Parse(sql string) (*core.Database, error) {
	stmtNodes, _, err := p.p.Parse(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse MySQL dump: %v", err)
	}

	db := &core.Database{
		Tables: []*core.Table{},
	}

	for _, stmtNode := range stmtNodes {
		if createStmt, ok := stmtNode.(*ast.CreateTableStmt); ok {
			table, err := p.convertCreateTable(createStmt)
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
		Name:        stmt.Table.Name.O,
		Columns:     []*core.Column{},
		Constraints: []*core.Constraint{},
		Indexes:     []*core.Index{},
	}

	for _, opt := range stmt.Options {
		switch opt.Tp {
		case ast.TableOptionComment:
			table.Comment = opt.StrValue
		case ast.TableOptionCharset:
			table.Charset = opt.StrValue
		case ast.TableOptionCollate:
			table.Collate = opt.StrValue
		case ast.TableOptionEngine:
			table.Engine = opt.StrValue
		case ast.TableOptionAutoIncrement:
			table.AutoIncrement = opt.UintValue
		case ast.TableOptionNone:
		case ast.TableOptionAutoIdCache:
		case ast.TableOptionAutoRandomBase:
		case ast.TableOptionAvgRowLength:
		case ast.TableOptionCheckSum:
		case ast.TableOptionCompression:
		case ast.TableOptionConnection:
		case ast.TableOptionPassword:
		case ast.TableOptionKeyBlockSize:
		case ast.TableOptionMaxRows:
		case ast.TableOptionMinRows:
		case ast.TableOptionDelayKeyWrite:
		case ast.TableOptionRowFormat:
		case ast.TableOptionStatsPersistent:
		case ast.TableOptionStatsAutoRecalc:
		case ast.TableOptionShardRowID:
		case ast.TableOptionPreSplitRegion:
		case ast.TableOptionPackKeys:
		case ast.TableOptionTablespace:
		case ast.TableOptionNodegroup:
		case ast.TableOptionDataDirectory:
		case ast.TableOptionIndexDirectory:
		case ast.TableOptionStorageMedia:
		case ast.TableOptionStatsSamplePages:
		case ast.TableOptionSecondaryEngine:
		case ast.TableOptionSecondaryEngineNull:
		case ast.TableOptionInsertMethod:
		case ast.TableOptionTableCheckSum:
		case ast.TableOptionUnion:
		case ast.TableOptionEncryption:
		case ast.TableOptionTTL:
		case ast.TableOptionTTLEnable:
		case ast.TableOptionTTLJobInterval:
		case ast.TableOptionEngineAttribute:
		case ast.TableOptionSecondaryEngineAttribute:
		case ast.TableOptionAutoextendSize:
		case ast.TableOptionPageChecksum:
		case ast.TableOptionPageCompressed:
		case ast.TableOptionPageCompressionLevel:
		case ast.TableOptionTransactional:
		case ast.TableOptionIetfQuotes:
		case ast.TableOptionSequence:
		case ast.TableOptionAffinity:
		case ast.TableOptionPlacementPolicy:
		case ast.TableOptionStatsBuckets:
		case ast.TableOptionStatsTopN:
		case ast.TableOptionStatsColsChoice:
		case ast.TableOptionStatsColList:
		case ast.TableOptionStatsSampleRate:
		}
	}

	for _, colDef := range stmt.Cols {
		col := &core.Column{
			Name:     colDef.Name.Name.O,
			TypeRaw:  colDef.Tp.String(),
			Type:     normalizeType(colDef.Tp.String()),
			Nullable: true,
			Collate:  colDef.Tp.GetCollate(),
			Charset:  colDef.Tp.GetCharset(),
		}

		for _, opt := range colDef.Options {
			switch opt.Tp {
			case ast.ColumnOptionNotNull:
				col.Nullable = false
			case ast.ColumnOptionNull:
				col.Nullable = true
			case ast.ColumnOptionPrimaryKey:
				col.PrimaryKey = true
			case ast.ColumnOptionAutoIncrement:
				col.AutoIncrement = true
			case ast.ColumnOptionDefaultValue:
				col.DefaultValue = p.exprToString(opt.Expr)
			case ast.ColumnOptionOnUpdate:
				col.OnUpdate = p.exprToString(opt.Expr)
			case ast.ColumnOptionUniqKey:
				table.Constraints = append(table.Constraints, &core.Constraint{
					Type:    core.Unique,
					Columns: []string{col.Name},
				})
			case ast.ColumnOptionComment:
				if s := p.exprToString(opt.Expr); s != nil {
					col.Comment = *s
				}
			case ast.ColumnOptionCollate:
				if s := p.exprToString(opt.Expr); s != nil {
					col.Collate = *s
				} else {
					col.Collate = opt.StrValue
				}
			case ast.ColumnOptionFulltext:
				table.Indexes = append(table.Indexes, &core.Index{
					Columns: []string{col.Name},
					Unique:  false,
					Type:    "FULLTEXT",
				})
			case ast.ColumnOptionCheck:
				if s := p.exprToString(opt.Expr); s != nil {
					table.Constraints = append(table.Constraints, &core.Constraint{
						Type:            core.Check,
						Columns:         []string{col.Name},
						CheckExpression: *s,
					})
				}
			case ast.ColumnOptionReference:
				c := &core.Constraint{
					Type:            core.ForeignKey,
					Columns:         []string{col.Name},
					ReferencedTable: opt.Refer.Table.Name.O,
				}
				refCols := make([]string, 0, len(opt.Refer.IndexPartSpecifications))
				for _, spec := range opt.Refer.IndexPartSpecifications {
					if spec.Column != nil {
						refCols = append(refCols, spec.Column.Name.O)
					}
				}
				c.ReferencedColumns = refCols
				if opt.Refer.OnDelete != nil {
					c.OnDelete = opt.Refer.OnDelete.ReferOpt.String()
				}
				if opt.Refer.OnUpdate != nil {
					c.OnUpdate = opt.Refer.OnUpdate.ReferOpt.String()
				}
				table.Constraints = append(table.Constraints, c)
			case ast.ColumnOptionGenerated:
				col.IsGenerated = true
				if opt.Expr != nil {
					if s := p.exprToString(opt.Expr); s != nil {
						col.GenerationExpression = *s
					}
				}
				if opt.Stored {
					col.GenerationStorage = "STORED"
				} else {
					col.GenerationStorage = "VIRTUAL"
				}
			case ast.ColumnOptionColumnFormat, ast.ColumnOptionStorage, ast.ColumnOptionAutoRandom, ast.ColumnOptionSecondaryEngineAttribute, ast.ColumnOptionNoOption:
			}
		}
		table.Columns = append(table.Columns, col)
	}

	for _, constraint := range stmt.Constraints {
		c := &core.Constraint{
			Name: constraint.Name,
		}

		columns := make([]string, 0, len(constraint.Keys))
		for _, key := range constraint.Keys {
			columns = append(columns, key.Column.Name.O)
		}
		c.Columns = columns

		switch constraint.Tp {
		case ast.ConstraintPrimaryKey:
			c.Type = core.PrimaryKey
			c.Name = "PRIMARY"

			for _, colName := range columns {
				if col := table.FindColumn(colName); col != nil {
					col.PrimaryKey = true
				}
			}
			table.Constraints = append(table.Constraints, c)

		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			c.Type = core.Unique
			table.Constraints = append(table.Constraints, c)

		case ast.ConstraintForeignKey:
			c.Type = core.ForeignKey
			c.ReferencedTable = constraint.Refer.Table.Name.O
			refCols := make([]string, 0, len(constraint.Refer.IndexPartSpecifications))
			for _, spec := range constraint.Refer.IndexPartSpecifications {
				if spec.Column != nil {
					refCols = append(refCols, spec.Column.Name.O)
				}
			}
			c.ReferencedColumns = refCols
			if constraint.Refer.OnDelete != nil {
				c.OnDelete = constraint.Refer.OnDelete.ReferOpt.String()
			}
			if constraint.Refer.OnUpdate != nil {
				c.OnUpdate = constraint.Refer.OnUpdate.ReferOpt.String()
			}
			table.Constraints = append(table.Constraints, c)

		case ast.ConstraintIndex, ast.ConstraintKey:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: columns,
				Unique:  false,
				Type:    "BTREE",
			})
		case ast.ConstraintFulltext:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: columns,
				Unique:  false,
				Type:    "FULLTEXT",
			})
		case ast.ConstraintCheck:
			c.Type = core.Check
			if constraint.Expr != nil {
				if s := p.exprToString(constraint.Expr); s != nil {
					c.CheckExpression = *s
				}
			}
			table.Constraints = append(table.Constraints, c)
		case ast.ConstraintVector, ast.ConstraintColumnar:
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    constraint.Name,
				Columns: columns,
				Unique:  false,
				Type:    "INDEX",
			})
		case ast.ConstraintNoConstraint:

		}
	}

	return table, nil
}

func (p *Parser) exprToString(expr ast.ExprNode) *string {
	if expr == nil {
		return nil
	}
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := expr.Restore(restoreCtx); err != nil {
		return nil
	}
	s := sb.String()

	if strings.Contains(s, "'") {
		start := strings.Index(s, "'")
		end := strings.LastIndex(s, "'")
		if start != -1 && end != -1 && start < end {
			s = s[start+1 : end]
		}
	}

	return &s
}

func normalizeType(rawType string) string {
	rawType = strings.ToLower(strings.TrimSpace(rawType))

	if strings.Contains(rawType, "char") || strings.Contains(rawType, "text") || strings.Contains(rawType, "string") || strings.Contains(rawType, "enum") || strings.Contains(rawType, "set") {
		return "string"
	}

	if strings.Contains(rawType, "int") {
		return "int"
	}

	if strings.Contains(rawType, "float") || strings.Contains(rawType, "double") || strings.Contains(rawType, "decimal") || strings.Contains(rawType, "numeric") {
		return "float"
	}

	if strings.Contains(rawType, "bool") || rawType == "tinyint(1)" {
		return "boolean"
	}

	if strings.Contains(rawType, "date") || strings.Contains(rawType, "time") || strings.Contains(rawType, "timestamp") {
		return "datetime"
	}

	if strings.Contains(rawType, "json") {
		return "json"
	}

	if strings.Contains(rawType, "uuid") {
		return "uuid"
	}

	return rawType
}
