package validate

import (
	"fmt"

	"smf/internal/core"
)

func Enums(db *core.Database) error {
	for _, table := range db.Tables {
		for _, col := range table.Columns {
			if err := ColumnEnums(col, table); err != nil {
				return err
			}
		}

		for _, con := range table.Constraints {
			if err := ConstraintEnums(con, table); err != nil {
				return err
			}
		}

		for _, idx := range table.Indexes {
			if err := IndexEnums(idx, table); err != nil {
				return err
			}
		}
	}
	return nil
}

func ColumnEnums(c *core.Column, table *core.Table) error {
	if err := ColumnType(c, table); err != nil {
		return err
	}
	if err := RefActions(c, table); err != nil {
		return err
	}
	if err := Generation(c, table); err != nil {
		return err
	}
	return Identity(c, table)
}

func ColumnType(c *core.Column, table *core.Table) error {
	if c.Type == "" {
		return nil
	}
	switch c.Type {
	case core.DataTypeString, core.DataTypeInt, core.DataTypeFloat, core.DataTypeBoolean,
		core.DataTypeDatetime, core.DataTypeJSON, core.DataTypeUUID, core.DataTypeBinary,
		core.DataTypeEnum, core.DataTypeUnknown:
		return nil
	default:
		return fmt.Errorf("table %q, column %q: invalid type %q", table.Name, c.Name, c.Type)
	}
}

func RefActions(c *core.Column, table *core.Table) error {
	if c.RefOnDelete != "" && !ReferentialAction(c.RefOnDelete) {
		return fmt.Errorf("table %q, column %q: invalid ref_on_delete %q", table.Name, c.Name, c.RefOnDelete)
	}
	if c.RefOnUpdate != "" && !ReferentialAction(c.RefOnUpdate) {
		return fmt.Errorf("table %q, column %q: invalid ref_on_update %q", table.Name, c.Name, c.RefOnUpdate)
	}
	return nil
}

func Generation(c *core.Column, table *core.Table) error {
	if c.IsGenerated && c.GenerationStorage != "" {
		switch c.GenerationStorage {
		case core.GenerationVirtual, core.GenerationStored:
		default:
			return fmt.Errorf("table %q, column %q: invalid generation_storage %q", table.Name, c.Name, c.GenerationStorage)
		}
	}
	return nil
}

func Identity(c *core.Column, table *core.Table) error {
	if c.IdentityGeneration != "" {
		switch c.IdentityGeneration {
		case core.IdentityAlways, core.IdentityByDefault:
		default:
			return fmt.Errorf("table %q, column %q: invalid identity_generation %q", table.Name, c.Name, c.IdentityGeneration)
		}
	}
	return nil
}

func ConstraintEnums(con *core.Constraint, table *core.Table) error {
	switch con.Type {
	case core.ConstraintPrimaryKey, core.ConstraintForeignKey, core.ConstraintUnique, core.ConstraintCheck:
	default:
		return fmt.Errorf("table %q, constraint %q: invalid constraint type %q", table.Name, con.Name, con.Type)
	}

	if con.Type == core.ConstraintForeignKey {
		if con.OnDelete != "" {
			if !ReferentialAction(con.OnDelete) {
				return fmt.Errorf("table %q, constraint %q: invalid on_delete %q", table.Name, con.Name, con.OnDelete)
			}
		}
		if con.OnUpdate != "" {
			if !ReferentialAction(con.OnUpdate) {
				return fmt.Errorf("table %q, constraint %q: invalid on_update %q", table.Name, con.Name, con.OnUpdate)
			}
		}
	}

	return nil
}

func IndexEnums(i *core.Index, table *core.Table) error {
	if err := IndexType(i, table); err != nil {
		return err
	}
	if err := IndexVisibility(i, table); err != nil {
		return err
	}
	return IndexColumnsOrder(i, table)
}

func IndexType(i *core.Index, table *core.Table) error {
	if i.Type == "" {
		return nil
	}
	switch i.Type {
	case core.IndexTypeBTree, core.IndexTypeHash, core.IndexTypeFullText, core.IndexTypeSpatial, core.IndexTypeGIN, core.IndexTypeGiST:
		return nil
	default:
		return fmt.Errorf("table %q, index %q: invalid index type %q", table.Name, i.Name, i.Type)
	}
}

func IndexVisibility(i *core.Index, table *core.Table) error {
	if i.Visibility == "" {
		return nil
	}
	switch i.Visibility {
	case core.IndexVisible, core.IndexInvisible:
		return nil
	default:
		return fmt.Errorf("table %q, index %q: invalid visibility %q", table.Name, i.Name, i.Visibility)
	}
}

func IndexColumnsOrder(i *core.Index, table *core.Table) error {
	for _, ic := range i.Columns {
		if ic.Order != "" {
			switch ic.Order {
			case core.SortAsc, core.SortDesc:
			default:
				return fmt.Errorf("table %q, index %q, column %q: invalid sort order %q", table.Name, i.Name, ic.Name, ic.Order)
			}
		}
	}
	return nil
}

func ReferentialAction(ra core.ReferentialAction) bool {
	switch ra {
	case core.RefActionNone, core.RefActionCascade, core.RefActionRestrict, core.RefActionSetNull, core.RefActionSetDefault, core.RefActionNoAction:
		return true
	default:
		return false
	}
}
