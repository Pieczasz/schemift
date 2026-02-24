package mysql

import (
	"context"
	"database/sql"
	"strings"

	"smf/internal/core"
)

func detectDialect(ctx context.Context, db *sql.DB) (core.Dialect, string, error) {
	var varName, comment string

	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'version_comment'").Scan(&varName, &comment)
	if err != nil {
		return "", "", err
	}

	comment = strings.ToLower(comment)

	version, err := getVersion(ctx, db)
	if err != nil {
		return "", "", err
	}

	switch {
	case strings.Contains(comment, "mariadb"):
		return core.DialectMariaDB, version, nil
	case strings.Contains(comment, "tidb"):
		return core.DialectTiDB, version, nil
	default:
		return core.DialectMySQL, version, nil
	}
}

func getVersion(ctx context.Context, db *sql.DB) (string, error) {
	var version string
	err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if err != nil {
		return "", err
	}
	if version, _, found := strings.Cut(version, "-"); found {
		return version, nil
	}
	return version, nil
}
