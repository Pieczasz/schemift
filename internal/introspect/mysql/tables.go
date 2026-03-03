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

func parseCreateTableDDL(dialect core.Dialect, tableName, _ string) (*core.Table, error) {
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

	return table, nil
}
