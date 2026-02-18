package mysql

import (
	"context"
	"database/sql"
	"smf/internal/core"
	"smf/internal/introspect"
)

func init() {
	introspect.Register(core.DialectMySQL, New)
	introspect.Register(core.DialectMariaDB, New)
	introspect.Register(core.DialectTiDB, New)
}

type introspecter struct{}

func New() introspect.Introspecter {
	return &introspecter{}
}

func (i *introspecter) Introspect(ctx context.Context, db *sql.DB) (*core.Database, error) {
	return nil, nil
}
