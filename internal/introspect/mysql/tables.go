package mysql

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"

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

	tables, err := queryTablesData(ic.ctx, ic, tableNames)
	if err != nil {
		return err
	}

	db.Tables = append(db.Tables, tables...)
	return nil
}

func queryTableNames(ic *introspectCtx) ([]string, error) {
	rows, err := ic.db.QueryContext(ic.ctx, "SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tableNames, nil
}

func queryTablesData(parentCtx context.Context, ic *introspectCtx, names []string) ([]*core.Table, error) {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	workers := computeWorkerCount(len(names))
	jobs := make(chan string)
	tables := make([]*core.Table, 0, len(names))

	var mu sync.Mutex
	var errOnce sync.Once
	var firstErr error
	var wg sync.WaitGroup
	setFirstErr := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	// Workers read table names from jobs until the channel is closed.
	// On the first error we store it once and cancel the shared context to stop all workers quickly.
	for range workers {
		wg.Go(func() {
			for tableName := range jobs {
				if ctx.Err() != nil {
					return
				}

				table, err := introspectTable(ctx, ic, tableName)
				if err != nil {
					setFirstErr(fmt.Errorf("introspect table %s: %w", tableName, err))
					return
				}

				mu.Lock()
				tables = append(tables, table)
				mu.Unlock()
			}
		})
	}

	// Producer: enqueue all table names unless work has already been canceled.
	go func() {
		defer close(jobs)
		for _, tableName := range names {
			select {
			case <-ctx.Done():
				return
			case jobs <- tableName:
			}
		}
	}()

	// Wait for all workers to finish after a producer closes jobs.
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return tables, nil
}

func computeWorkerCount(n int) int {
	if n <= 0 {
		return 0
	}
	workers := min(n, max(2, runtime.NumCPU()))
	return min(workers, 8)
}

func introspectTable(ctx context.Context, ic *introspectCtx, tableName string) (*core.Table, error) {
	ddl, err := queryShowCreateTable(ctx, ic, tableName)
	if err != nil {
		return nil, fmt.Errorf("show create table: %w", err)
	}

	table, err := parseCreateTableDDL(ic.dialect, tableName, ddl)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	return table, nil
}

func queryShowCreateTable(ctx context.Context, ic *introspectCtx, tableName string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE %s", quoteIdentifier(tableName))
	var ignored string
	var ddl string
	if err := ic.db.QueryRowContext(ctx, query).Scan(&ignored, &ddl); err != nil {
		return "", err
	}
	return ddl, nil
}

func quoteIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, "`", "``")
	return "`" + escaped + "`"
}

// bodyItemKind classifies a single item inside the CREATE TABLE body.
type bodyItemKind int

const (
	bodyItemColumn     bodyItemKind = iota // Column definition (starts with identifier token).
	bodyItemConstraint                     // Table-level PK, UNIQUE, FK, CHECK, or named CONSTRAINT.
	bodyItemIndex                          // Inline index: KEY, INDEX, FULLTEXT, SPATIAL.
)

// ddlSections holds the three top-level sections of a CREATE TABLE statement.
type ddlSections struct {
	header string // Everything before the opening '(' (e.g. "CREATE TABLE `t`").
	body   string // Content inside the outer parentheses.
	tail   string // Table options after the closing ')'.
}

// splitDDLSections splits a CREATE TABLE statement into header, body, and tail.
func splitDDLSections(ddl string) (ddlSections, error) {
	openIdx := -1
	for i, ch := range ddl {
		if ch == '(' {
			openIdx = i
			break
		}
	}
	if openIdx == -1 {
		return ddlSections{}, fmt.Errorf("missing opening parenthesis in CREATE TABLE DDL")
	}

	depth := 0
	closeIdx := -1
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false

	for i := openIdx; i < len(ddl); i++ {
		ch := ddl[i]

		if (inSingleQuote || inDoubleQuote) && ch == '\\' {
			i++
			continue
		}

		switch {
		case ch == '\'' && !inDoubleQuote && !inBacktick:
			inSingleQuote = !inSingleQuote
		case ch == '"' && !inSingleQuote && !inBacktick:
			inDoubleQuote = !inDoubleQuote
		case ch == '`' && !inSingleQuote && !inDoubleQuote:
			inBacktick = !inBacktick
		case !inSingleQuote && !inDoubleQuote && !inBacktick:
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
				if depth == 0 {
					closeIdx = i
					break
				}
			}
		}

		if closeIdx != -1 {
			break
		}
	}

	if closeIdx == -1 {
		return ddlSections{}, fmt.Errorf("unbalanced parentheses in CREATE TABLE DDL")
	}

	return ddlSections{
		header: strings.TrimSpace(ddl[:openIdx]),
		body:   ddl[openIdx+1 : closeIdx],
		tail:   strings.TrimSpace(ddl[closeIdx+1:]),
	}, nil
}

// splitBodyItems splits the body of a CREATE TABLE statement by top-level
func splitBodyItems(body string) []string {
	var items []string
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	start := 0

	for i := 0; i < len(body); i++ {
		ch := body[i]

		if (inSingleQuote || inDoubleQuote) && ch == '\\' {
			i++
			continue
		}

		switch {
		case ch == '\'' && !inDoubleQuote && !inBacktick:
			inSingleQuote = !inSingleQuote
		case ch == '"' && !inSingleQuote && !inBacktick:
			inDoubleQuote = !inDoubleQuote
		case ch == '`' && !inSingleQuote && !inDoubleQuote:
			inBacktick = !inBacktick
		case !inSingleQuote && !inDoubleQuote && !inBacktick:
			switch ch {
			case '(':
				depth++
			case ')':
				depth--
			case ',':
				if depth == 0 {
					items = append(items, strings.TrimSpace(body[start:i]))
					start = i + 1
				}
			}
		}
	}

	if last := strings.TrimSpace(body[start:]); last != "" {
		items = append(items, last)
	}

	return items
}

// classifyBodyItem determines whether a body item is a column definition,
func classifyBodyItem(item string) bodyItemKind {
	upper := strings.ToUpper(strings.TrimSpace(item))

	if strings.HasPrefix(upper, "CONSTRAINT") {
		return bodyItemConstraint
	}

	for _, prefix := range []string{
		"PRIMARY KEY",
		"UNIQUE KEY", "UNIQUE INDEX", "UNIQUE ",
		"FOREIGN KEY",
		"CHECK",
	} {
		if strings.HasPrefix(upper, prefix) {
			return bodyItemConstraint
		}
	}

	for _, prefix := range []string{
		"KEY", "INDEX",
		"FULLTEXT KEY", "FULLTEXT INDEX", "FULLTEXT",
		"SPATIAL KEY", "SPATIAL INDEX", "SPATIAL",
	} {
		if strings.HasPrefix(upper, prefix) {
			return bodyItemIndex
		}
	}

	return bodyItemColumn
}

func parseCreateTableDDL(dialect core.Dialect, tableName, ddl string) (*core.Table, error) {
	table := &core.Table{
		Name:        tableName,
		Columns:     make([]*core.Column, 0),
		Constraints: make([]*core.Constraint, 0),
		Indexes:     make([]*core.Index, 0),
	}

	switch dialect {
	case core.DialectMySQL:
		table.Options.MySQL = &core.MySQLTableOptions{}
	case core.DialectMariaDB:
		table.Options.MySQL = &core.MySQLTableOptions{}
		table.Options.MariaDB = &core.MariaDBTableOptions{}
	case core.DialectTiDB:
		table.Options.MySQL = &core.MySQLTableOptions{}
		table.Options.TiDB = &core.TiDBTableOptions{}
	}

	sections, err := splitDDLSections(ddl)
	if err != nil {
		return nil, err
	}

	items := splitBodyItems(sections.body)

	for _, item := range items {
		switch classifyBodyItem(item) {
		case bodyItemColumn:
			col, err := parseColumn(dialect, item)
			if err != nil {
				return nil, fmt.Errorf("parse column: %w", err)
			}
			table.Columns = append(table.Columns, col)

		case bodyItemConstraint:
			constraint, err := parseConstraint(dialect, item)
			if err != nil {
				return nil, fmt.Errorf("parse constraint: %w", err)
			}
			table.Constraints = append(table.Constraints, constraint)

		case bodyItemIndex:
			idx, err := parseIndex(dialect, item)
			if err != nil {
				return nil, fmt.Errorf("parse index: %w", err)
			}
			table.Indexes = append(table.Indexes, idx)
		}
	}

	// TODO: parse sections.tail for table options (ENGINE, CHARSET, etc.)
	_ = sections.tail

	return table, nil
}
