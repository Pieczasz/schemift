package postgresql

import (
	"context"
	"database/sql"
	"smf/internal/core"
	"smf/internal/introspect"
)

func init() {
	introspect.Register(core.DialectPostgreSQL, New)
}

type postgresqlIntrospecter struct{}

func New() introspect.Introspecter {
	return &postgresqlIntrospecter{}
}

func (i *postgresqlIntrospecter) Introspect(ctx context.Context, db *sql.DB) (*core.Database, error) {
	return nil, nil
}
