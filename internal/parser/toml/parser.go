// Package toml provides a parser for the smf TOML schema format.
// It reads a dialect-agnostic schema definition from a .toml file and
// converts it into the canonical core.Database representation that the
// rest of the smf toolchain operates on.
package toml

import (
	"fmt"
	"io"
	"os"

	"github.com/BurntSushi/toml"

	"smf/internal/core"
)

// schemaFile is the top-level TOML document.
type schemaFile struct {
	Database tomlDatabase `toml:"database"`
}

// tomlDatabase maps [database].
type tomlDatabase struct {
	Name    string      `toml:"name"`
	Dialect string      `toml:"dialect"`
	Tables  []tomlTable `toml:"tables"`
}

// tomlTable maps [[database.tables]].
type tomlTable struct {
	Name        string           `toml:"name"`
	Comment     string           `toml:"comment"`
	Options     tomlTableOptions `toml:"options"`
	Columns     []tomlColumn     `toml:"columns"`
	Constraints []tomlConstraint `toml:"constraints"`
	Indexes     []tomlIndex      `toml:"indexes"`
}

// tomlTableOptions maps [database.tables.options].
type tomlTableOptions struct {
	Engine       string `toml:"engine"`
	Charset      string `toml:"charset"`
	Collate      string `toml:"collate"`
	RowFormat    string `toml:"row_format"`
	Tablespace   string `toml:"tablespace"`
	Compression  string `toml:"compression"`
	Encryption   string `toml:"encryption"`
	KeyBlockSize uint64 `toml:"key_block_size"`
}

// tomlColumn maps [[database.tables.columns]].
type tomlColumn struct {
	Name                 string `toml:"name"`
	Type                 string `toml:"type"`
	TypeRaw              string `toml:"type_raw"`
	PrimaryKey           bool   `toml:"primary_key"`
	AutoIncrement        bool   `toml:"auto_increment"`
	Nullable             bool   `toml:"nullable"`
	DefaultValue         string `toml:"default_value"`
	OnUpdate             string `toml:"on_update"`
	Comment              string `toml:"comment"`
	Collate              string `toml:"collate"`
	Charset              string `toml:"charset"`
	IsGenerated          bool   `toml:"is_generated"`
	GenerationExpression string `toml:"generation_expression"`
	GenerationStorage    string `toml:"generation_storage"` // "VIRTUAL" or "STORED"
}

// tomlConstraint maps [[database.tables.constraints]].
type tomlConstraint struct {
	Name              string   `toml:"name"`
	Type              string   `toml:"type"`
	Columns           []string `toml:"columns"`
	ReferencedTable   string   `toml:"referenced_table"`
	ReferencedColumns []string `toml:"referenced_columns"`
	OnDelete          string   `toml:"on_delete"`
	OnUpdate          string   `toml:"on_update"`
	CheckExpression   string   `toml:"check_expression"`
	Enforced          *bool    `toml:"enforced"` // pointer so we can distinguish absent (-> true) from explicit false
}

// tomlIndex maps [[database.tables.indexes]].
type tomlIndex struct {
	Name       string            `toml:"name"`
	Unique     bool              `toml:"unique"`
	Type       string            `toml:"type"`
	Comment    string            `toml:"comment"`
	Visibility string            `toml:"visibility"`
	Columns    []tomlIndexColumn `toml:"columns"`
}

// tomlIndexColumn maps [[database.tables.indexes.columns]].
type tomlIndexColumn struct {
	Name   string `toml:"name"`
	Length int    `toml:"length"`
	Order  string `toml:"order"`
}

// Parser reads smf TOML schema files.
type Parser struct{}

// NewParser creates a new TOML schema parser.
func NewParser() *Parser {
	return &Parser{}
}

// ParseFile opens the file at the given path and parses it as a TOML schema.
func (p *Parser) ParseFile(path string) (*core.Database, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("toml: open file %q: %w", path, err)
	}
	defer f.Close()

	return p.Parse(f)
}

// Parse reads TOML content from r and returns the corresponding core.Database.
func (p *Parser) Parse(r io.Reader) (*core.Database, error) {
	var sf schemaFile
	if _, err := toml.NewDecoder(r).Decode(&sf); err != nil {
		return nil, fmt.Errorf("toml: decode error: %w", err)
	}
	return convertDatabase(&sf.Database)
}

func convertDatabase(td *tomlDatabase) (*core.Database, error) {
	db := &core.Database{
		Name:    td.Name,
		Dialect: td.Dialect,
		Tables:  make([]*core.Table, 0, len(td.Tables)),
	}

	for i := range td.Tables {
		t, err := convertTable(&td.Tables[i])
		if err != nil {
			return nil, fmt.Errorf("toml: table %q: %w", td.Tables[i].Name, err)
		}
		db.Tables = append(db.Tables, t)
	}

	if err := db.Validate(); err != nil {
		return nil, fmt.Errorf("toml: schema validation failed: %w", err)
	}

	return db, nil
}

func convertTable(tt *tomlTable) (*core.Table, error) {
	table := &core.Table{
		Name:    tt.Name,
		Comment: tt.Comment,
		Options: convertTableOptions(&tt.Options),
	}

	table.Columns = make([]*core.Column, 0, len(tt.Columns))
	for i := range tt.Columns {
		col := convertColumn(&tt.Columns[i])
		table.Columns = append(table.Columns, col)
	}

	table.Constraints = make([]*core.Constraint, 0, len(tt.Constraints))
	for i := range tt.Constraints {
		c := convertConstraint(&tt.Constraints[i])
		table.Constraints = append(table.Constraints, c)
	}

	table.Indexes = make([]*core.Index, 0, len(tt.Indexes))
	for i := range tt.Indexes {
		idx := convertIndex(&tt.Indexes[i])
		table.Indexes = append(table.Indexes, idx)
	}

	return table, nil
}

func convertTableOptions(to *tomlTableOptions) core.TableOptions {
	return core.TableOptions{
		Engine:       to.Engine,
		Charset:      to.Charset,
		Collate:      to.Collate,
		RowFormat:    to.RowFormat,
		Tablespace:   to.Tablespace,
		Compression:  to.Compression,
		Encryption:   to.Encryption,
		KeyBlockSize: to.KeyBlockSize,
	}
}

func convertColumn(tc *tomlColumn) *core.Column {
	col := &core.Column{
		Name:          tc.Name,
		Nullable:      tc.Nullable,
		PrimaryKey:    tc.PrimaryKey,
		AutoIncrement: tc.AutoIncrement,
		Comment:       tc.Comment,
		Collate:       tc.Collate,
		Charset:       tc.Charset,
	}

	if tc.TypeRaw != "" {
		col.TypeRaw = tc.TypeRaw
	} else {
		col.TypeRaw = tc.Type
	}

	col.Type = core.NormalizeDataType(col.TypeRaw)

	if tc.DefaultValue != "" {
		v := tc.DefaultValue
		col.DefaultValue = &v
	}
	if tc.OnUpdate != "" {
		v := tc.OnUpdate
		col.OnUpdate = &v
	}

	// Generated columns.
	col.IsGenerated = tc.IsGenerated
	col.GenerationExpression = tc.GenerationExpression
	if tc.GenerationStorage != "" {
		col.GenerationStorage = core.GenerationStorage(tc.GenerationStorage)
	}

	return col
}

func convertConstraint(tc *tomlConstraint) *core.Constraint {
	c := &core.Constraint{
		Name:              tc.Name,
		Type:              core.ConstraintType(tc.Type),
		Columns:           tc.Columns,
		ReferencedTable:   tc.ReferencedTable,
		ReferencedColumns: tc.ReferencedColumns,
		OnDelete:          core.ReferentialAction(tc.OnDelete),
		OnUpdate:          core.ReferentialAction(tc.OnUpdate),
		CheckExpression:   tc.CheckExpression,
	}

	if tc.Enforced != nil {
		c.Enforced = *tc.Enforced
	} else {
		c.Enforced = true
	}

	return c
}

func convertIndex(ti *tomlIndex) *core.Index {
	idx := &core.Index{
		Name:    ti.Name,
		Unique:  ti.Unique,
		Comment: ti.Comment,
	}

	if ti.Type != "" {
		idx.Type = core.IndexType(ti.Type)
	} else {
		idx.Type = core.IndexTypeBTree
	}

	if ti.Visibility != "" {
		idx.Visibility = core.IndexVisibility(ti.Visibility)
	} else {
		idx.Visibility = core.IndexVisible
	}

	idx.Columns = make([]core.IndexColumn, 0, len(ti.Columns))
	for i := range ti.Columns {
		ic := convertIndexColumn(&ti.Columns[i])
		idx.Columns = append(idx.Columns, ic)
	}

	return idx
}

func convertIndexColumn(tc *tomlIndexColumn) core.IndexColumn {
	ic := core.IndexColumn{
		Name:   tc.Name,
		Length: tc.Length,
	}

	if tc.Order != "" {
		ic.Order = core.SortOrder(tc.Order)
	} else {
		ic.Order = core.SortAsc
	}

	return ic
}
