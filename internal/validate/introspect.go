package validate

import (
	"errors"
	"regexp"
	"strings"
)

var (
	ErrEmptyTableName   = errors.New("table name cannot be empty")
	ErrTableNameTooLong = errors.New("table name exceeds maximum length of 64 characters")
	ErrInvalidTableName = errors.New("table name contains invalid characters")

	ErrEmptyColumnName   = errors.New("column name cannot be empty")
	ErrColumnNameTooLong = errors.New("column name exceeds maximum length of 64 characters")
	ErrInvalidColumnName = errors.New("column name contains invalid characters")
)

const (
	MaxIdentifierLength = 64
)

var validIdentifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func Identifier(name string) bool {
	if name == "" {
		return false
	}
	if len(name) > MaxIdentifierLength {
		return false
	}
	return validIdentifierRegex.MatchString(name)
}

func QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func TableName(name string) error {
	if name == "" {
		return ErrEmptyTableName
	}
	if len(name) > MaxIdentifierLength {
		return ErrTableNameTooLong
	}
	if !Identifier(name) {
		return ErrInvalidTableName
	}
	return nil
}

func TableNames(names []string) error {
	for _, name := range names {
		if err := TableName(name); err != nil {
			return err
		}
	}
	return nil
}

func ColumnName(name string) error {
	if name == "" {
		return ErrEmptyColumnName
	}
	if len(name) > MaxIdentifierLength {
		return ErrColumnNameTooLong
	}
	if !Identifier(name) {
		return ErrInvalidColumnName
	}
	return nil
}

func CountParens(s string) int {
	count := 0
	for _, c := range s {
		switch c {
		case '(':
			count++
		case ')':
			count--
		}
	}
	return count
}
