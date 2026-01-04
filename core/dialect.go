package core

type Dialect string

const (
	DialectMySQL      Dialect = "mysql"
	DialectPostgreSQL Dialect = "postgresql"
	DialectSQLite     Dialect = "sqlite"
	DialectMSSQL      Dialect = "mssql"
	DialectOracle     Dialect = "oracle"
)

type SQLGenerator interface {
	GenerateMigration(diff *SchemaDiff) *Migration
	QuoteIdentifier(name string) string
	QuoteString(value string) string
	FormatValue(value string) string
}

type SchemaParser interface {
	Parse(sql string) (*Database, error)
}

type BreakingChangeDetector interface {
	DetectBreakingChanges(diff *SchemaDiff) []BreakingChange
}

type MigrationOptions struct {
	Dialect              Dialect
	IncludeDrops         bool
	IncludeUnsafe        bool
	TransactionMode      TransactionMode
	PreserveForeignKeys  bool
	DeferForeignKeyCheck bool
}

type TransactionMode int

const (
	TransactionNone TransactionMode = iota
	TransactionSingle
	TransactionPerStatement
)

func DefaultMigrationOptions(dialect Dialect) MigrationOptions {
	return MigrationOptions{
		Dialect:              dialect,
		IncludeDrops:         true,
		IncludeUnsafe:        false,
		TransactionMode:      TransactionSingle,
		PreserveForeignKeys:  true,
		DeferForeignKeyCheck: true,
	}
}
