package mysql

import (
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

	tableData, err := gatherTableData(ic, tableNames)
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		t := buildTable(tableName, tableData[tableName])
		db.Tables = append(db.Tables, t)
	}

	return nil
}

type tableData struct {
	tableOptions tableOptions
	columns      []*core.Column
	indexes      []*core.Index
	constraints  map[string]*sqlRawConstraint
}

func gatherTableData(ic *introspectCtx, tableNames []string) (map[string]tableData, error) {
	placeholders := make([]string, len(tableNames))
	args := make([]any, len(tableNames))
	for i, item := range tableNames {
		placeholders[i] = "?"
		args[i] = item
	}

	tableOptions, err := queryTableOptions(ic, placeholders, args)
	if err != nil {
		return nil, err
	}

	columns, err := queryAllColumns(ic, placeholders, args)
	if err != nil {
		return nil, err
	}

	indexes, err := queryAllIndexes(ic, placeholders, args)
	if err != nil {
		return nil, err
	}

	constraints, err := queryAllConstraints(ic, placeholders, args)
	if err != nil {
		return nil, err
	}

	result := make(map[string]tableData)
	for _, tableName := range tableNames {
		result[tableName] = tableData{
			tableOptions: tableOptions[tableName],
			columns:      columns[tableName],
			indexes:      indexes[tableName],
			constraints:  constraints[tableName],
		}
	}
	return result, nil
}

func buildTable(tableName string, data tableData) *core.Table {
	t := &core.Table{
		Name:        tableName,
		Comment:     data.tableOptions.comment,
		Options:     core.TableOptions{},
		Columns:     []*core.Column{},
		Constraints: []*core.Constraint{},
		Indexes:     []*core.Index{},
	}

	opts := data.tableOptions
	t.Options.MySQL = &core.MySQLTableOptions{
		Engine:        opts.engine,
		Charset:       opts.charset,
		Collate:       opts.collate,
		AutoIncrement: opts.autoIncrement,
	}

	t.Columns = append(t.Columns, data.columns...)
	t.Indexes = append(t.Indexes, data.indexes...)

	for _, c := range data.constraints {
		t.Constraints = append(t.Constraints, convertToCoreConstraint(c))
	}

	return t
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
