package core

import (
	"fmt"
	"regexp"
)

// validateColumn checks a single column for structural correctness.
func validateColumn(col *Column, rules *ValidationRules, nameRe *regexp.Regexp) error {
	if err := validateName(col.Name, "column", rules, nameRe, false); err != nil {
		return err
	}

	// TODO: validate this field (col.DefaultValue)
	// TODO: validate this field (col.OnUpdate)
	// TODO: validate this field (col.Comment)
	// TODO: validate this field (col.Collate)
	// TODO: validate this field (col.Charset)
	// TODO: validate this field (col.EnumValues)
	// TODO: validate this field (col.IdentitySeed)
	// TODO: validate this field (col.IdentityIncrement)
	// TODO: validate this field (col.SequenceName)
	if err := validateColumnOptions(col); err != nil {
		return err
	}

	if col.References != "" {
		if _, _, ok := ParseReferences(col.References); !ok {
			return fmt.Errorf("invalid references %q: expected format \"table.column\"", col.References)
		}
	}

	return nil
}

func validateColumnOptions(col *Column) error {
	if col.MySQL != nil {
		// TODO: validate this field (col.MySQL.ColumnFormat)
		// TODO: validate this field (col.MySQL.Storage)
		// TODO: validate this field (col.MySQL.SecondaryEngineAttribute)
	}
	return nil
}
