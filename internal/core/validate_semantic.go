package core

import (
	"fmt"
	"slices"
)

// validateLogicalRules validates dialect-specific and logical rules that are not
// strictly structural.
func (db *Database) validateLogicalRules() error {
	dialect := *db.Dialect
	for _, table := range db.Tables {
		for _, col := range table.Columns {
			if err := col.validateLogicalRules(table, dialect); err != nil {
				return err
			}
		}
		if err := table.validateForeignKeyTypeCompatibility(db); err != nil {
			return err
		}
	}
	return nil
}

func (c *Column) validateLogicalRules(table *Table, dialect Dialect) error {
	if c.RawType != "" {
		if err := ValidateRawType(c.RawType, dialect); err != nil {
			return fmt.Errorf("table %q, column %q: %w", table.Name, c.Name, err)
		}
	}
	// TODO: validate this field (col.Nullable)
	// TODO: validate this field (col.Unique)
	// TODO: validate this field (col.DefaultValue)
	if err := c.validateAutoIncrement(table, dialect); err != nil {
		return err
	}
	if err := c.validatePrimaryKey(table); err != nil {
		return err
	}
	if err := c.validateGenerationSemantic(table); err != nil {
		return err
	}
	if err := c.validateIdentitySemantic(table); err != nil {
		return err
	}
	return c.validateDialectSpecificSemantic(table, dialect)
}

func (c *Column) validateAutoIncrement(table *Table, dialect Dialect) error {
	if c.AutoIncrement && c.Type != DataTypeInt {
		return fmt.Errorf("table %q, column %q: auto_increment is only allowed on integer columns", table.Name, c.Name)
	}
	if dialect == DialectSQLite && c.AutoIncrement && !c.PrimaryKey {
		return fmt.Errorf("table %q, column %q: SQLite AUTOINCREMENT is only allowed on PRIMARY KEY columns", table.Name, c.Name)
	}

	return nil
}

func (c *Column) validatePrimaryKey(table *Table) error {
	if (c.PrimaryKey || table.PartOfPrimaryKey(c.Name)) && c.Nullable {
		return fmt.Errorf("table %q, column %q: primary key columns cannot be nullable", table.Name, c.Name)
	}
	return nil
}

func (c *Column) validateGenerationSemantic(table *Table) error {
	if c.IsGenerated && c.GenerationExpression == "" {
		return fmt.Errorf("table %q, column %q: generated column must have an expression", table.Name, c.Name)
	}
	return nil
}

func (c *Column) validateIdentitySemantic(table *Table) error {
	if (c.IdentitySeed != 0 || c.IdentityIncrement != 0) && !c.AutoIncrement {
		return fmt.Errorf("table %q, column %q: identity_seed and identity_increment can only be set for auto_increment columns", table.Name, c.Name)
	}
	return nil
}

func (c *Column) validateDialectSpecificSemantic(table *Table, dialect Dialect) error {
	if dialect == DialectTiDB {
		if c.TiDB != nil && c.TiDB.ShardBits > 0 {
			if !c.PrimaryKey || c.Type != DataTypeInt {
				return fmt.Errorf("table %q, column %q: TiDB AUTO_RANDOM can only be applied to BIGINT PRIMARY KEY columns", table.Name, c.Name)
			}
		}
	}
	return nil
}

// validateForeignKeyTypeCompatibility ensures that referencing and referenced columns in a
// Foreign Key have compatible types.
func (t *Table) validateForeignKeyTypeCompatibility(db *Database) error {
	for _, con := range t.Constraints {
		if con.Type != ConstraintForeignKey {
			continue
		}
		refTable := db.FindTable(con.ReferencedTable)
		if refTable == nil {
			continue
		}
		for i, colName := range con.Columns {
			if i >= len(con.ReferencedColumns) {
				continue
			}
			refColName := con.ReferencedColumns[i]
			col := t.FindColumn(colName)
			refCol := refTable.FindColumn(refColName)
			if col != nil && refCol != nil && col.Type != refCol.Type {
				return fmt.Errorf("table %q, constraint %q: type mismatch between referencing column %q (%s) and referenced column %q (%s) in table %q",
					t.Name, con.Name, colName, col.Type, refColName, refCol.Type, con.ReferencedTable)
			}
		}
	}
	return nil
}

// PartOfPrimaryKey checks if a column name is included in any PRIMARY KEY constraint
// defined at the table level.
func (t *Table) PartOfPrimaryKey(colName string) bool {
	for _, con := range t.Constraints {
		if con.Type == ConstraintPrimaryKey {
			if slices.Contains(con.Columns, colName) {
				return true
			}
		}
	}
	return false
}
