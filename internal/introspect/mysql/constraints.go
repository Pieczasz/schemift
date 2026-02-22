package mysql

import (
	"database/sql"
	"maps"
	"strings"

	"smf/internal/core"
	"smf/internal/validate"
)

type sqlRawConstraint struct {
	name              string
	constraintType    string
	columns           []string
	referencedTable   sql.NullString
	referencedColumns []string
	onDelete          sql.NullString
	onUpdate          sql.NullString
	checkExpression   sql.NullString
	enforced          bool
}

func queryAllConstraints(ic *introspectCtx, tableNames []string) (map[string]map[string]*sqlRawConstraint, error) {
	placeholders := make([]string, len(tableNames))
	args := make([]any, len(tableNames))
	for i, name := range tableNames {
		placeholders[i] = "?"
		args[i] = name
	}

	query := `
		SELECT
			table_name,
			constraint_name,
			constraint_type,
			ENFORCED
		FROM information_schema.table_constraints
		WHERE table_schema = DATABASE() AND table_name IN (` + strings.Join(placeholders, ",") + `)
		ORDER BY table_name, constraint_name
	`

	rows, err := ic.db.QueryContext(ic.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]map[string]*sqlRawConstraint)
	for rows.Next() {
		var tableName, name, ctype string
		var enforced sql.NullString
		if err := rows.Scan(&tableName, &name, &ctype, &enforced); err != nil {
			return nil, err
		}

		if result[tableName] == nil {
			result[tableName] = make(map[string]*sqlRawConstraint)
		}

		result[tableName][name] = &sqlRawConstraint{
			name:           name,
			constraintType: ctype,
			enforced:       strings.ToUpper(enforced.String) == "YES",
		}
	}

	if err := queryAllConstraintColumns(ic, tableNames, result); err != nil {
		return nil, err
	}

	if err := queryAllForeignKeyInfo(ic, tableNames, result); err != nil {
		return nil, err
	}

	return result, rows.Err()
}

func queryAllConstraintColumns(ic *introspectCtx, tableNames []string, constraints map[string]map[string]*sqlRawConstraint) error {
	if len(tableNames) == 0 {
		return nil
	}

	placeholders := make([]string, len(tableNames))
	args := make([]any, len(tableNames))
	for i, name := range tableNames {
		placeholders[i] = "?"
		args[i] = name
	}

	query := `
		SELECT
			table_name,
			constraint_name,
			column_name,
			ordinal_position
		FROM information_schema.key_column_usage
		WHERE table_schema = DATABASE() AND table_name IN (` + strings.Join(placeholders, ",") + `)
		ORDER BY table_name, constraint_name, ordinal_position
	`

	rows, err := ic.db.QueryContext(ic.ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, constraintName, column string
		var ordinal int
		if err := rows.Scan(&tableName, &constraintName, &column, &ordinal); err != nil {
			return err
		}

		if tableConstraints, ok := constraints[tableName]; ok {
			if c, ok := tableConstraints[constraintName]; ok {
				c.columns = append(c.columns, column)
			}
		}
	}

	return rows.Err()
}

func queryAllForeignKeyInfo(ic *introspectCtx, tableNames []string, constraints map[string]map[string]*sqlRawConstraint) error {
	if len(tableNames) == 0 {
		return nil
	}

	placeholders := make([]string, len(tableNames))
	args := make([]any, len(tableNames))
	for i, name := range tableNames {
		placeholders[i] = "?"
		args[i] = name
	}

	query := `
		SELECT
			fk.table_name,
			fk.constraint_name,
			fk.column_name,
			fk.ordinal_position,
			rc.unique_constraint_schema,
			rc.referenced_table_name,
			rc.delete_rule,
			rc.update_rule
		FROM information_schema.referential_constraints rc
		JOIN information_schema.key_column_usage fk
			ON rc.constraint_name = fk.constraint_name
			AND rc.constraint_schema = fk.table_schema
			AND rc.table_name = fk.table_name
		WHERE rc.constraint_schema = DATABASE()
			AND rc.table_name IN (` + strings.Join(placeholders, ",") + `)
		ORDER BY rc.table_name, rc.constraint_name, fk.ordinal_position
	`

	rows, err := ic.db.QueryContext(ic.ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	currentFK := ""
	var refColumns []string

	for rows.Next() {
		var tableName, constraintName, column, refTable, uniqueSchema string
		var ordinal int
		var deleteRule, updateRule sql.NullString
		if err := rows.Scan(&tableName, &constraintName, &column, &ordinal, &uniqueSchema, &refTable, &deleteRule, &updateRule); err != nil {
			return err
		}

		if tableConstraints, ok := constraints[tableName]; ok {
			if c, ok := tableConstraints[constraintName]; ok {
				if currentFK != constraintName {
					if currentFK != "" {
						if prev, ok := constraints[tableName][currentFK]; ok {
							prev.referencedColumns = refColumns
						}
					}
					currentFK = constraintName
					refColumns = nil
					c.referencedTable = sql.NullString{String: refTable, Valid: true}
					c.onDelete = deleteRule
					c.onUpdate = updateRule
				}
				refColumns = append(refColumns, column)
			}
		}
	}

	if currentFK != "" {
		if prev, ok := constraints[""][currentFK]; ok {
			prev.referencedColumns = refColumns
		}
	}

	return rows.Err()
}

func queryAllCheckConstraints(ic *introspectCtx, tableNames []string) (map[string]string, error) {
	result := make(map[string]string)

	for _, tableName := range tableNames {
		if err := validate.TableName(tableName); err != nil {
			continue
		}

		var createStmt string
		quotedName := validate.QuoteIdentifier(tableName)
		err := ic.db.QueryRowContext(ic.ctx, "SHOW CREATE TABLE "+quotedName).Scan(&tableName, &createStmt)
		if err != nil {
			continue
		}

		checkConstraints := parseCheckConstraints(createStmt)
		maps.Copy(result, checkConstraints)
	}

	return result, nil
}

func parseCheckConstraints(createStmt string) map[string]string {
	result := make(map[string]string)

	// Look for CONSTRAINT `name` CHECK (expression) pattern
	// First, find all CONSTRAINT positions
	constraintStarts := findAllOccurrences(createStmt, "CONSTRAINT `")

	for _, constraintStart := range constraintStarts {
		// Extract constraint name
		nameStart := constraintStart + len("CONSTRAINT `")
		nameEnd := strings.Index(createStmt[nameStart:], "`")
		if nameEnd == -1 {
			continue
		}
		name := createStmt[nameStart : nameStart+nameEnd]

		// Find CHECK after this constraint
		checkStart := constraintStart + strings.Index(createStmt[constraintStart:], "CHECK (")
		if checkStart < constraintStart || checkStart > constraintStart+200 {
			continue
		}

		// Find matching closing parenthesis
		exprStart := checkStart + len("CHECK (")
		exprEnd := findMatchingParenIndex(createStmt, exprStart)
		if exprEnd == -1 {
			continue
		}

		expr := createStmt[exprStart:exprEnd]
		result[name] = expr
	}

	return result
}

func findAllOccurrences(s, substr string) []int {
	var result []int
	start := 0
	for {
		idx := strings.Index(s[start:], substr)
		if idx == -1 {
			break
		}
		result = append(result, start+idx)
		start = start + idx + len(substr)
	}
	return result
}

func findMatchingParenIndex(s string, start int) int {
	count := 1
	for i := start; i < len(s); i++ {
		switch s[i] {
		case '(':
			count++
		case ')':
			count--
			if count == 0 {
				return i
			}
		}
	}
	return -1
}

func convertToCoreConstraint(c *sqlRawConstraint) *core.Constraint {
	cc := &core.Constraint{
		Name:            c.name,
		Type:            parseConstraintType(c.constraintType),
		Columns:         c.columns,
		ReferencedTable: c.referencedTable.String,
	}

	if c.referencedTable.Valid {
		cc.ReferencedColumns = c.referencedColumns
		cc.OnDelete = parseReferentialAction(c.onDelete.String)
		cc.OnUpdate = parseReferentialAction(c.onUpdate.String)
	}

	if c.checkExpression.Valid {
		cc.CheckExpression = c.checkExpression.String
		cc.Enforced = c.enforced
	}

	return cc
}

func parseConstraintType(t string) core.ConstraintType {
	switch strings.ToUpper(t) {
	case "PRIMARY KEY":
		return core.ConstraintPrimaryKey
	case "FOREIGN KEY":
		return core.ConstraintForeignKey
	case "UNIQUE":
		return core.ConstraintUnique
	case "CHECK":
		return core.ConstraintCheck
	default:
		return core.ConstraintCheck
	}
}

func parseReferentialAction(action string) core.ReferentialAction {
	switch strings.ToUpper(action) {
	case "CASCADE":
		return core.RefActionCascade
	case "RESTRICT":
		return core.RefActionRestrict
	case "SET NULL":
		return core.RefActionSetNull
	case "SET DEFAULT":
		return core.RefActionSetDefault
	case "NO ACTION":
		return core.RefActionNoAction
	default:
		return core.RefActionNone
	}
}
