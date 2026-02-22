package mysql

import (
	"database/sql"
	"strings"

	"smf/internal/core"
)

func queryAllColumns(ic *introspectCtx, tableNames []string) (map[string][]*core.Column, error) {
	placeholders := make([]string, len(tableNames))
	args := make([]any, len(tableNames))
	for i, name := range tableNames {
		placeholders[i] = "?"
		args[i] = name
	}

	query := `
		SELECT
			c.table_name,
			c.column_name,
			c.column_type,
			c.column_comment,
			c.is_nullable,
			c.column_default,
			c.extra,
			c.character_set_name,
			c.collation_name,
			c.column_key,
			c.generation_expression
		FROM information_schema.columns c
		WHERE c.table_schema = DATABASE() AND c.table_name IN (` + strings.Join(placeholders, ",") + `)
		ORDER BY c.table_name, c.ordinal_position
	`

	rows, err := ic.db.QueryContext(ic.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]*core.Column)
	currentTable := ""

	for rows.Next() {
		var tableName string
		var name, colType, comment, nullable, defaultVal, extra, charset, collation, colKey, genExpr sql.NullString
		if err := rows.Scan(&tableName, &name, &colType, &comment, &nullable, &defaultVal, &extra, &charset, &collation, &colKey, &genExpr); err != nil {
			return nil, err
		}

		if currentTable != tableName {
			currentTable = tableName
			result[tableName] = []*core.Column{}
		}

		isPK := colKey.String == "PRI"
		isAutoInc := strings.Contains(extra.String, "auto_increment")

		col := &core.Column{
			Name:          name.String,
			RawType:       colType.String,
			Type:          core.NormalizeDataType(colType.String),
			Nullable:      nullable.String == "YES",
			PrimaryKey:    isPK,
			AutoIncrement: isAutoInc,
			Comment:       comment.String,
			Charset:       charset.String,
			Collate:       strings.ReplaceAll(collation.String, charset.String+"_", ""),
		}

		if defaultVal.Valid {
			col.DefaultValue = &defaultVal.String
		}

		if genExpr.Valid {
			col.IsGenerated = true
			col.GenerationExpression = genExpr.String
			col.GenerationStorage = core.GenerationStored
		}

		result[tableName] = append(result[tableName], col)
	}

	return result, rows.Err()
}
