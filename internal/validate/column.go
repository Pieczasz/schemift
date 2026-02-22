package validate

import (
	"fmt"
	"regexp"

	"smf/internal/core"
)

func Column(c *core.Column, rules *core.ValidationRules, nameRe *regexp.Regexp) error {
	if err := Name(c.Name, rules, nameRe, false); err != nil {
		return fmt.Errorf("column %q: %w", c.Name, err)
	}

	if c.Type == "" && c.RawType == "" || c.Type == core.DataTypeUnknown {
		return fmt.Errorf("column %q: type is empty", c.Name)
	}

	if err := ColumnOptions(c); err != nil {
		return fmt.Errorf("column %q: %w", c.Name, err)
	}

	if c.References != "" {
		if _, _, ok := core.ParseReferences(c.References); !ok {
			return fmt.Errorf("column %q: invalid references %q: expected format \"table.column\"", c.Name, c.References)
		}
	}

	return nil
}

func ColumnOptions(_ *core.Column) error {
	return nil
}
