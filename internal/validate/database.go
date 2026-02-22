// Package validate contains various validation logic e.g., for database, tables structure, dialect
// specific logic, columns. It makes sure basic schema structs are valid and we can perform, diffing,
// and migration logic.
package validate

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"smf/internal/core"
)

func Database(db *core.Database) error {
	if err := RequiredFields(db); err != nil {
		return err
	}

	nameRe, err := CompileAllowedNamePattern(db.Validation)
	if err != nil {
		return err
	}

	if err := TableUniqueness(db.Tables); err != nil {
		return err
	}

	if err := SynthesizeConstraints(db.Tables); err != nil {
		return err
	}

	if err := TableStructures(db.Tables, db.Validation, nameRe); err != nil {
		return err
	}

	if err := ForeignKeys(db.Tables); err != nil {
		return err
	}

	if err := LogicalRules(db.Tables, *db.Dialect); err != nil {
		return err
	}

	if err := Enums(db); err != nil {
		return err
	}

	return nil
}

func RequiredFields(db *core.Database) error {
	if db == nil {
		return errors.New("database is nil")
	}
	if db.Dialect == nil {
		return fmt.Errorf("dialect is required; supported dialects: %v", core.SupportedDialects())
	}
	if !core.ValidDialect(string(*db.Dialect)) {
		return fmt.Errorf("unsupported dialect %q; supported dialects: %v", *db.Dialect, core.SupportedDialects())
	}
	if strings.TrimSpace(db.Name) == "" {
		return errors.New("database name is required")
	}
	if len(db.Tables) == 0 {
		return errors.New("schema is empty, declare some tables first")
	}
	return nil
}

func CompileAllowedNamePattern(rules *core.ValidationRules) (*regexp.Regexp, error) {
	if rules == nil || rules.AllowedNamePattern == "" {
		return nil, nil
	}
	re, err := regexp.Compile(rules.AllowedNamePattern)
	if err != nil {
		return nil, fmt.Errorf("invalid allowed_name_pattern %q: %w", rules.AllowedNamePattern, err)
	}
	return re, nil
}

var snakeCaseRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func SnakeCase(s string) bool {
	return snakeCaseRe.MatchString(s)
}

func Name(name string, rules *core.ValidationRules, nameRe *regexp.Regexp, useTableLength bool) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("name is empty")
	}

	if !SnakeCase(name) {
		return fmt.Errorf("%q must be in snake_case", name)
	}

	if rules == nil {
		return nil
	}

	maxLen := rules.MaxColumnNameLength
	if useTableLength {
		maxLen = rules.MaxTableNameLength
	}
	if maxLen > 0 && len(name) > maxLen {
		return fmt.Errorf("%q exceeds maximum length %d", name, maxLen)
	}
	if nameRe != nil && !nameRe.MatchString(name) {
		return fmt.Errorf("%q does not match allowed pattern %q", name, nameRe.String())
	}
	return nil
}
