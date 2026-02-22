package mysql

import (
	"database/sql"
	"strings"

	"smf/internal/core"
)

func introspectTables(ic *introspectCtx, db *core.Database) error {
	tableNames, err := queryTableNames(ic)
	if err != nil {
		return err
	}

	if len(tableNames) == 0 {
		return nil
	}

	tableOptions, err := queryAllTableOptions(ic, tableNames)
	if err != nil {
		return err
	}

	columns, err := queryAllColumns(ic, tableNames)
	if err != nil {
		return err
	}

	indexes, err := queryAllIndexes(ic, tableNames)
	if err != nil {
		return err
	}

	constraints, err := queryAllConstraints(ic, tableNames)
	if err != nil {
		return err
	}

	checkConstraints, err := queryAllCheckConstraints(ic, tableNames)
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		t := &core.Table{
			Name:        tableName,
			Comment:     tableOptions[tableName].comment,
			Options:     core.TableOptions{},
			Columns:     []*core.Column{},
			Constraints: []*core.Constraint{},
			Indexes:     []*core.Index{},
		}

		opts := tableOptions[tableName]
		t.Options.MySQL = &core.MySQLTableOptions{
			Engine:        opts.engine,
			Charset:       opts.charset,
			Collate:       opts.collate,
			AutoIncrement: opts.autoIncrement,
		}

		t.Columns = append(t.Columns, columns[tableName]...)

		t.Indexes = append(t.Indexes, indexes[tableName]...)

		for _, c := range constraints[tableName] {
			if c.checkExpression.Valid {
				if expr, ok := checkConstraints[c.name]; ok {
					c.checkExpression = sql.NullString{String: expr, Valid: true}
				}
			}
			t.Constraints = append(t.Constraints, convertToCoreConstraint(c))
		}

		db.Tables = append(db.Tables, t)
	}

	return nil
}

type tableOptions struct {
	engine        string
	charset       string
	collate       string
	autoIncrement uint64
	comment       string
}

func queryTableNames(ic *introspectCtx) ([]string, error) {
	rows, err := ic.db.QueryContext(ic.ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}

	return names, rows.Err()
}

func queryAllTableOptions(ic *introspectCtx, tableNames []string) (map[string]tableOptions, error) {
	if len(tableNames) == 0 {
		return make(map[string]tableOptions), nil
	}

	placeholders := make([]string, len(tableNames))
	args := make([]any, len(tableNames))
	for i, name := range tableNames {
		placeholders[i] = "?"
		args[i] = name
	}

	query := `
		SELECT table_name, engine, table_collation, auto_increment, table_comment
		FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name IN (` + strings.Join(placeholders, ",") + `)
	`

	rows, err := ic.db.QueryContext(ic.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]tableOptions)
	for rows.Next() {
		var name, engine, collate, comment string
		var autoIncrement sql.NullInt64
		if err := rows.Scan(&name, &engine, &collate, &autoIncrement, &comment); err != nil {
			return nil, err
		}

		charset := ""
		if idx := strings.Index(collate, "_"); idx > 0 {
			charset = collate[:idx]
			collate = collate[idx+1:]
		}

		result[name] = tableOptions{
			engine:        engine,
			charset:       charset,
			collate:       collate,
			autoIncrement: uint64(autoIncrement.Int64),
			comment:       comment,
		}
	}

	return result, rows.Err()
}
