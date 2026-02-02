package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSQLParser(t *testing.T) {
	p := NewSQLParser()
	assert.NotNil(t, p)
	assert.NotNil(t, p.mysqlParser)
}

func TestSQLParserParseSchema(t *testing.T) {
	sql := `CREATE TABLE t (id INT);`

	p := NewSQLParser()
	db, err := p.ParseSchema(sql)
	require.NoError(t, err)
	require.NotNil(t, db)
	require.Len(t, db.Tables, 1)
	assert.Equal(t, "t", db.Tables[0].Name)
}

func TestSQLParserParseSchemaError(t *testing.T) {
	sql := `INVALID SQL;`

	p := NewSQLParser()
	db, err := p.ParseSchema(sql)
	assert.Error(t, err)
	assert.Nil(t, db)
}
