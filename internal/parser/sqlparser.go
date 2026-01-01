package parser

import (
	"strings"

	"schemift/internal/core"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

type SQLParser struct{}

func NewSQLParser() *SQLParser {
	return &SQLParser{}
}

func (p *SQLParser) ParseSchema(sql string) (*core.Database, error) {
	db := &core.Database{
		Tables: []*core.Table{},
	}

	pieces := strings.Split(sql, ";")

	for _, piece := range pieces {
		piece = strings.TrimSpace(piece)
		if piece == "" {
			continue
		}

		stmt, err := sqlparser.Parse(piece)
		if err != nil {
			continue
		}

		createTable, ok := stmt.(*sqlparser.CreateTable)
		if !ok {
			continue
		}

		table := &core.Table{
			Name:        createTable.Table.Name.String(),
			Columns:     []*core.Column{},
			Constraints: []*core.Constraint{},
			Indexes:     []*core.Index{},
		}

		for _, col := range createTable.Columns {
			c := &core.Column{
				Name:          col.Name.String(),
				TypeRaw:       col.Type.Type,
				Nullable:      !col.Type.NotNull,
				AutoIncrement: col.Type.Autoincrement,
			}
			if col.Type.Default != nil {
				val := string(col.Type.Default.Val)
				c.DefaultValue = &val
			}
			table.Columns = append(table.Columns, c)
		}

		for _, constraint := range createTable.Constraints {
			cols := extractColNames(constraint.Keys)

			switch constraint.Type {
			case sqlparser.ConstraintNoConstraint:
				continue

			case sqlparser.ConstraintPrimaryKey:
				for _, c := range table.Columns {
					for _, pkName := range cols {
						if strings.EqualFold(c.Name, pkName) {
							c.PrimaryKey = true
						}
					}
				}
				table.Constraints = append(table.Constraints, &core.Constraint{
					Name:    "PRIMARY",
					Type:    core.PrimaryKey,
					Columns: cols,
				})

			case sqlparser.ConstraintUniq, sqlparser.ConstraintUniqKey, sqlparser.ConstraintUniqIndex:
				table.Constraints = append(table.Constraints, &core.Constraint{
					Name:    constraint.Name,
					Type:    core.Unique,
					Columns: cols,
				})

			case sqlparser.ConstraintForeignKey:
				refTable := ""
				refCol := ""
				if constraint.Reference != nil {
					refTable = constraint.Reference.Table.Name.String()
					refCols := extractColNames(constraint.Reference.Columns)
					if len(refCols) > 0 {
						refCol = refCols[0]
					}
				}
				table.Constraints = append(table.Constraints, &core.Constraint{
					Name:             constraint.Name,
					Type:             core.ForeignKey,
					Columns:          cols,
					ReferencedTable:  refTable,
					ReferencedColumn: refCol,
					OnDelete:         string(constraint.OnDelete),
					OnUpdate:         string(constraint.OnUpdate),
				})

			case sqlparser.ConstraintKey, sqlparser.ConstraintIndex:
				table.Indexes = append(table.Indexes, &core.Index{
					Name:    constraint.Name,
					Columns: cols,
					Unique:  false,
					Type:    "BTREE",
				})

			case sqlparser.ConstraintFulltext:
				table.Indexes = append(table.Indexes, &core.Index{
					Name:    constraint.Name,
					Columns: cols,
					Unique:  false,
					Type:    "FULLTEXT",
				})
			}
		}

		for _, idx := range createTable.Indexes {
			if idx.Info.Primary || idx.Info.Unique {
				continue
			}
			table.Indexes = append(table.Indexes, &core.Index{
				Name:    idx.Info.Name.String(),
				Columns: extractColNames(idx.Columns),
				Unique:  false,
				Type:    "BTREE",
			})
		}

		db.Tables = append(db.Tables, table)
	}

	return db, nil
}

func extractColNames(cols []sqlparser.ColIdent) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.String()
	}
	return names
}
