package mysql

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func TestNewColumnFromDefAndTypeNormalization(t *testing.T) {
	sql := `CREATE TABLE t (c1 VARCHAR(10));`
	p := NewParser()

	stmtNodes, _, err := p.p.Parse(sql, "", "")
	require.NoError(t, err)
	require.NotEmpty(t, stmtNodes)

	create, ok := stmtNodes[0].(*ast.CreateTableStmt)
	require.True(t, ok)
	require.NotEmpty(t, create.Cols)

	col := newColumnFromDef(create.Cols[0])
	assert.Equal(t, "c1", col.Name)
	assert.Contains(t, col.TypeRaw, "varchar")
	assert.Equal(t, core.DataTypeString, col.Type)
	assert.True(t, col.Nullable)
}

func TestParseColumnsAndPrimaryKeyConstraint(t *testing.T) {
	sql := `CREATE TABLE t (
		id INT PRIMARY KEY,
		name VARCHAR(10)
	);`

	table := parseSingleTable(t, sql)
	col := table.Columns[0]
	assert.True(t, col.PrimaryKey)
	assert.False(t, col.Nullable)

	pk := table.PrimaryKey()
	require.NotNil(t, pk)
	assert.Equal(t, []string{"id"}, pk.Columns)
}

func TestParseColumnsAndDefaultUnquote(t *testing.T) {
	sql := `CREATE TABLE t (
		c1 VARCHAR(10) DEFAULT 'abc',
		c2 VARCHAR(10) DEFAULT _utf8'xyz'
	);`

	table := parseSingleTable(t, sql)
	cols := table.Columns
	require.Len(t, cols, 2)

	require.NotNil(t, cols[0].DefaultValue)
	require.NotNil(t, cols[1].DefaultValue)
	assert.Equal(t, "abc", *cols[0].DefaultValue)
	assert.Equal(t, "xyz", *cols[1].DefaultValue)
}

func TestParseColumnsAndGeneratedAndCheck(t *testing.T) {
	sql := `CREATE TABLE t (
		c1 INT,
		c2 INT GENERATED ALWAYS AS (c1 + 1) STORED,
		c3 INT CHECK (c3 > 0)
	);`

	table := parseSingleTable(t, sql)

	gen := table.Columns[1]
	assert.True(t, gen.IsGenerated)
	assert.Equal(t, core.GenerationStored, gen.GenerationStorage)
	assert.Contains(t, gen.GenerationExpression, "c1")
	assert.Contains(t, gen.GenerationExpression, "+1")

	var check *core.Constraint
	for _, c := range table.Constraints {
		if c.Type == core.ConstraintCheck {
			check = c
			break
		}
	}
	require.NotNil(t, check)
	assert.Contains(t, check.CheckExpression, "c3")
	assert.Contains(t, check.CheckExpression, ">0")
}

func TestParseColumnsAndInlineForeignKey(t *testing.T) {
	sql := `CREATE TABLE child (
		parent_id INT REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT
	);`

	table := parseSingleTable(t, sql)

	var fk *core.Constraint
	for _, c := range table.Constraints {
		if c.Type == core.ConstraintForeignKey {
			fk = c
			break
		}
	}
	require.NotNil(t, fk)
	assert.Equal(t, []string{"parent_id"}, fk.Columns)
	assert.Equal(t, "parent", fk.ReferencedTable)
	assert.Equal(t, []string{"id"}, fk.ReferencedColumns)
	assert.Equal(t, core.RefActionCascade, fk.OnDelete)
	assert.Equal(t, core.RefActionRestrict, fk.OnUpdate)
}

func TestParseColumnsAndFulltextAndUnique(t *testing.T) {
	sql := `CREATE TABLE t (
		c1 TEXT UNIQUE KEY,
		c2 TEXT,
		FULLTEXT KEY idx_c2 (c2)
	);`

	table := parseSingleTable(t, sql)

	var unique *core.Constraint
	for _, c := range table.Constraints {
		if c.Type == core.ConstraintUnique {
			unique = c
			break
		}
	}
	require.NotNil(t, unique)
	assert.Equal(t, []string{"c1"}, unique.Columns)

	var fulltext *core.Index
	for _, idx := range table.Indexes {
		if idx.Type == core.IndexTypeFullText {
			fulltext = idx
			break
		}
	}
	require.NotNil(t, fulltext)
	require.Len(t, fulltext.Columns, 1)
	assert.Equal(t, "c2", fulltext.Columns[0].Name)
}

func TestTryUnquoteSQLStringLiteralWithIntroducer(t *testing.T) {
	value, ok := tryUnquoteSQLStringLiteral("_utf8'hello''world'")
	assert.True(t, ok)
	assert.Equal(t, "hello'world", value)
}

func TestApplyColumnOptionVariants(t *testing.T) {
	p := NewParser()
	table := &core.Table{Columns: []*core.Column{{Name: "id"}}}
	col := &core.Column{Name: "c1", Nullable: true}

	p.applyColumnOption(table, col, nil)
	assert.True(t, col.Nullable)

	p.applyColumnOption(table, col, &ast.ColumnOption{Tp: ast.ColumnOptionNotNull})
	assert.False(t, col.Nullable)

	p.applyColumnOption(table, col, &ast.ColumnOption{Tp: ast.ColumnOptionNull})
	assert.True(t, col.Nullable)

	p.applyColumnOption(table, col, &ast.ColumnOption{Tp: ast.ColumnOptionOnUpdate, Expr: ast.NewValueExpr("CURRENT_TIMESTAMP", "", "")})
	require.NotNil(t, col.OnUpdate)

	p.applyColumnOption(table, col, &ast.ColumnOption{Tp: ast.ColumnOptionColumnFormat, StrValue: "FIXED"})
	assert.Equal(t, "FIXED", col.ColumnFormat)

	p.applyColumnOption(table, col, &ast.ColumnOption{Tp: ast.ColumnOptionStorage, StrValue: "DISK"})
	assert.Equal(t, "DISK", col.Storage)

	p.applyColumnOption(table, col, &ast.ColumnOption{Tp: ast.ColumnOptionAutoRandom, AutoRandOpt: ast.AutoRandomOption{ShardBits: 7}})
	assert.Equal(t, uint64(7), col.AutoRandom)

	p.applyColumnOption(table, col, &ast.ColumnOption{Tp: ast.ColumnOptionSecondaryEngineAttribute, StrValue: "attr"})
	assert.Equal(t, "attr", col.SecondaryEngineAttribute)

	p.applyColumnOption(table, col, &ast.ColumnOption{Tp: ast.ColumnOptionCollate, StrValue: "utf8mb4_bin"})
	assert.Equal(t, "utf8mb4_bin", col.Collate)

	p.applyColumnOption(table, col, &ast.ColumnOption{Tp: ast.ColumnOptionComment, Expr: ast.NewValueExpr("note", "", "")})
	assert.Equal(t, "note", col.Comment)
}

func TestApplyGeneratedColumnOptionVirtual(t *testing.T) {
	p := NewParser()
	col := &core.Column{Name: "c1"}

	p.applyGeneratedColumnOption(col, &ast.ColumnOption{Tp: ast.ColumnOptionGenerated, Expr: ast.NewValueExpr("c1+1", "", ""), Stored: false})
	assert.True(t, col.IsGenerated)
	assert.Equal(t, core.GenerationVirtual, col.GenerationStorage)
	assert.Contains(t, col.GenerationExpression, "c1")
}

func TestAddCheckConstraintNoExpr(t *testing.T) {
	p := NewParser()
	table := &core.Table{}

	p.addCheckConstraintForColumn(table, "c1", &ast.ColumnOption{Tp: ast.ColumnOptionCheck})
	assert.Len(t, table.Constraints, 0)
}

func TestAddFulltextIndexForColumn(t *testing.T) {
	p := NewParser()
	table := &core.Table{}

	p.addFulltextIndexForColumn(table, "body")
	require.Len(t, table.Indexes, 1)
	assert.Equal(t, core.IndexTypeFullText, table.Indexes[0].Type)
	assert.Equal(t, "body", table.Indexes[0].Columns[0].Name)
}

func TestApplyColumnCollateOptionWithExpr(t *testing.T) {
	p := NewParser()
	col := &core.Column{Name: "c1"}

	p.applyColumnCollateOption(col, &ast.ColumnOption{Tp: ast.ColumnOptionCollate, Expr: ast.NewValueExpr("utf8mb4_bin", "", "")})
	assert.Equal(t, "utf8mb4_bin", col.Collate)
}

func TestEnsurePrimaryKeyColumnAndReuse(t *testing.T) {
	p := NewParser()
	table := &core.Table{
		Columns: []*core.Column{{Name: "id", Nullable: true}},
		Constraints: []*core.Constraint{
			{Type: core.ConstraintPrimaryKey, Name: "", Columns: []string{"id"}},
		},
	}

	p.ensurePrimaryKeyColumn(table, "ID")
	pk := table.PrimaryKey()
	require.NotNil(t, pk)
	assert.Equal(t, "PRIMARY", pk.Name)
	assert.Equal(t, []string{"id"}, pk.Columns)
	assert.True(t, table.Columns[0].PrimaryKey)
	assert.False(t, table.Columns[0].Nullable)

	p.ensurePrimaryKeyColumn(table, " ")
	assert.Equal(t, []string{"id"}, pk.Columns)
}

func TestMarkColumnAsPrimaryKeyMissingColumn(t *testing.T) {
	p := NewParser()
	table := &core.Table{}

	p.markColumnAsPrimaryKey(table, "missing")
	assert.Len(t, table.Columns, 0)
}

func TestConstraintColumnsAndApplyConstraintVariants(t *testing.T) {
	p := NewParser()
	table := &core.Table{Columns: []*core.Column{{Name: "id"}, {Name: "name"}}}

	cols, idxCols := constraintColumns(nil)
	assert.Nil(t, cols)
	assert.Nil(t, idxCols)

	p.applyConstraint(table, nil, nil, nil)

	p.applyConstraint(table, &ast.Constraint{Tp: ast.ConstraintPrimaryKey}, []string{"id"}, nil)
	pk := table.PrimaryKey()
	require.NotNil(t, pk)
	assert.Equal(t, []string{"id"}, pk.Columns)

	p.applyConstraint(table, &ast.Constraint{Tp: ast.ConstraintUniq, Name: "uniq_name"}, []string{"name"}, nil)
	assert.NotNil(t, table.FindConstraint("uniq_name"))

	fk := &ast.Constraint{
		Tp:   ast.ConstraintForeignKey,
		Name: "fk_parent",
		Refer: &ast.ReferenceDef{
			Table: &ast.TableName{Name: ast.NewCIStr("parent")},
			IndexPartSpecifications: []*ast.IndexPartSpecification{
				{Column: &ast.ColumnName{Name: ast.NewCIStr("id")}},
			},
			OnDelete: &ast.OnDeleteOpt{ReferOpt: ast.ReferOptionCascade},
			OnUpdate: &ast.OnUpdateOpt{ReferOpt: ast.ReferOptionSetNull},
		},
	}
	p.applyConstraint(table, fk, []string{"id"}, nil)
	assert.Equal(t, "parent", table.Constraints[len(table.Constraints)-1].ReferencedTable)

	idxCols = []core.IndexColumn{{Name: "name", Length: 5}}
	p.applyConstraint(table, &ast.Constraint{Tp: ast.ConstraintIndex, Name: "idx_name"}, nil, idxCols)
	assert.NotNil(t, table.FindIndex("idx_name"))

	p.applyConstraint(table, &ast.Constraint{Tp: ast.ConstraintFulltext, Name: "ft_name"}, nil, idxCols)
	assert.NotNil(t, table.FindIndex("ft_name"))

	p.applyConstraint(table, &ast.Constraint{Tp: ast.ConstraintVector, Name: "vec_name"}, nil, idxCols)
	assert.NotNil(t, table.FindIndex("vec_name"))

	p.applyConstraint(table, &ast.Constraint{Tp: ast.ConstraintCheck, Name: "chk_name"}, []string{"id"}, nil)
	assert.NotNil(t, table.FindConstraint("chk_name"))

	check := &ast.Constraint{Tp: ast.ConstraintCheck, Name: "chk_expr", Expr: ast.NewValueExpr("id>0", "", "")}
	p.applyConstraint(table, check, []string{"id"}, nil)
	found := table.FindConstraint("chk_expr")
	require.NotNil(t, found)
	assert.Contains(t, found.CheckExpression, "id")
}

func TestExprToStringAndStringIntroducers(t *testing.T) {
	p := NewParser()

	assert.Nil(t, p.exprToString(nil))

	value := p.exprToString(ast.NewValueExpr("hello", "", ""))
	require.NotNil(t, value)
	assert.Equal(t, "hello", *value)

	value = p.exprToString(ast.NewValueExpr(123, "", ""))
	require.NotNil(t, value)
	assert.Equal(t, "123", *value)

	unquoted, ok := tryUnquoteSQLStringLiteral("N'hello''world'")
	assert.True(t, ok)
	assert.Equal(t, "hello'world", unquoted)

	_, ok = tryUnquoteSQLStringLiteral("no_quotes")
	assert.False(t, ok)

	_, ok = tryUnquoteSQLStringLiteral("x'abc'")
	assert.False(t, ok)

	_, ok = tryUnquoteSQLStringLiteral("N'abc")
	assert.False(t, ok)

	_, ok = tryUnquoteSQLStringLiteral("_bad-'x'")
	assert.False(t, ok)

	assert.True(t, isSQLStringIntroducer("N"))
	assert.True(t, isSQLStringIntroducer("_utf8"))
	assert.False(t, isSQLStringIntroducer("_"))
	assert.False(t, isSQLStringIntroducer("_bad-"))
	assert.False(t, isSQLStringIntroducer(""))
}

func parseSingleTable(t *testing.T, sql string) *core.Table {
	t.Helper()
	p := NewParser()
	db, err := p.Parse(sql)
	require.NoError(t, err)
	require.NotNil(t, db)
	require.Len(t, db.Tables, 1)
	return db.Tables[0]
}
