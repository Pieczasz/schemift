package mysql

import (
	"database/sql"
	"strings"

	"smf/internal/core"
)

func queryAllIndexes(ic *introspectCtx, tableNames []string) (map[string][]*core.Index, error) {
	placeholders := make([]string, len(tableNames))
	args := make([]any, len(tableNames))
	for i, name := range tableNames {
		placeholders[i] = "?"
		args[i] = name
	}

	query := `
		SELECT
			i.table_name,
			i.index_name,
			i.non_unique,
			i.index_type,
			i.comment,
			GROUP_CONCAT(CONCAT(i.column_name, IF(i.sub_part IS NULL, '', CONCAT('(', i.sub_part, ')'))) ORDER BY i.seq_in_index SEPARATOR ', ')
		FROM information_schema.statistics i
		WHERE i.table_schema = DATABASE() AND i.table_name IN (` + strings.Join(placeholders, ",") + `)
		GROUP BY i.table_name, i.index_name, i.non_unique, i.index_type, i.comment
		ORDER BY i.table_name, i.index_name
	`

	rows, err := ic.db.QueryContext(ic.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]*core.Index)
	currentTable := ""

	for rows.Next() {
		var tableName string
		var indexName, unique, indexType, comment, columns sql.NullString
		if err := rows.Scan(&tableName, &indexName, &unique, &indexType, &comment, &columns); err != nil {
			return nil, err
		}

		if currentTable != tableName {
			currentTable = tableName
			result[tableName] = []*core.Index{}
		}

		idx := &core.Index{
			Name:    indexName.String,
			Unique:  unique.String == "0",
			Type:    normalizeIndexType(indexType.String),
			Comment: comment.String,
		}

		for col := range strings.SplitSeq(columns.String, ", ") {
			if col != "" {
				idx.Columns = append(idx.Columns, core.ColumnIndex{Name: col})
			}
		}

		result[tableName] = append(result[tableName], idx)
	}

	return result, rows.Err()
}

func normalizeIndexType(t string) core.IndexType {
	switch strings.ToUpper(t) {
	case "BTREE":
		return core.IndexTypeBTree
	case "HASH":
		return core.IndexTypeHash
	case "FULLTEXT":
		return core.IndexTypeFullText
	case "SPATIAL":
		return core.IndexTypeSpatial
	default:
		return core.IndexTypeBTree
	}
}
