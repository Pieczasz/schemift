package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"smf/internal/core"
)

func TestFormatValueEmptyString(t *testing.T) {
	g := NewMySQLGenerator()

	result := g.formatValue("")
	assert.Equal(t, "''", result)
}

func TestFormatValueWhitespace(t *testing.T) {
	g := NewMySQLGenerator()

	result := g.formatValue("   ")
	assert.Equal(t, "''", result)
}

func TestFormatValueNullKeyword(t *testing.T) {
	g := NewMySQLGenerator()

	tests := []struct {
		input    string
		expected string
	}{
		{"NULL", "NULL"},
		{"null", "NULL"},
		{"Null", "NULL"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := g.formatValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatValueCurrentTimestamp(t *testing.T) {
	g := NewMySQLGenerator()

	tests := []struct {
		input    string
		expected string
	}{
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"},
		{"current_timestamp", "CURRENT_TIMESTAMP"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := g.formatValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatValueCurrentDate(t *testing.T) {
	g := NewMySQLGenerator()

	result := g.formatValue("CURRENT_DATE")
	assert.Equal(t, "CURRENT_DATE", result)
}

func TestFormatValueCurrentTime(t *testing.T) {
	g := NewMySQLGenerator()

	result := g.formatValue("CURRENT_TIME")
	assert.Equal(t, "CURRENT_TIME", result)
}

func TestFormatValueNow(t *testing.T) {
	g := NewMySQLGenerator()

	result := g.formatValue("NOW()")
	assert.Equal(t, "NOW()", result)
}

func TestFormatValueTrueFalse(t *testing.T) {
	g := NewMySQLGenerator()

	tests := []struct {
		input    string
		expected string
	}{
		{"TRUE", "TRUE"},
		{"true", "TRUE"},
		{"FALSE", "FALSE"},
		{"false", "FALSE"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := g.formatValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatValueNumericInteger(t *testing.T) {
	g := NewMySQLGenerator()

	tests := []struct {
		input    string
		expected string
	}{
		{"42", "42"},
		{"0", "0"},
		{"-1", "-1"},
		{"1000000", "1000000"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := g.formatValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatValueNumericFloat(t *testing.T) {
	g := NewMySQLGenerator()

	tests := []struct {
		input    string
		expected string
	}{
		{"3.14", "3.14"},
		{"0.5", "0.5"},
		{"-2.5", "-2.5"},
		{"1e10", "1e10"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := g.formatValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatValueFunctionCall(t *testing.T) {
	g := NewMySQLGenerator()

	tests := []struct {
		input    string
		expected string
	}{
		{"UUID()", "UUID()"},
		{"RAND()", "RAND()"},
		{"CONCAT('a', 'b')", "CONCAT('a', 'b')"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := g.formatValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatValueStringValue(t *testing.T) {
	g := NewMySQLGenerator()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple string", "hello", "'hello'"},
		{"string with spaces", "hello world", "'hello world'"},
		{"email", "test@example.com", "'test@example.com'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := g.formatValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatColumns(t *testing.T) {
	g := NewMySQLGenerator()

	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{"single column", []string{"id"}, "(`id`)"},
		{"multiple columns", []string{"id", "name"}, "(`id`, `name`)"},
		{"empty columns", []string{}, "()"},
		{"columns with whitespace", []string{" id ", " name "}, "(`id`, `name`)"},
		{"columns with empty", []string{"id", "", "name"}, "(`id`, `name`)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := g.formatColumns(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatIndexColumns(t *testing.T) {
	g := NewMySQLGenerator()

	tests := []struct {
		name     string
		input    []core.IndexColumn
		expected string
	}{
		{
			"single column",
			[]core.IndexColumn{{Name: "id"}},
			"(`id`)",
		},
		{
			"multiple columns",
			[]core.IndexColumn{{Name: "id"}, {Name: "name"}},
			"(`id`, `name`)",
		},
		{
			"column with length",
			[]core.IndexColumn{{Name: "name", Length: 10}},
			"(`name`(10))",
		},
		{
			"mixed columns",
			[]core.IndexColumn{{Name: "id"}, {Name: "description", Length: 50}},
			"(`id`, `description`(50))",
		},
		{
			"empty columns",
			[]core.IndexColumn{},
			"()",
		},
		{
			"column with whitespace name",
			[]core.IndexColumn{{Name: " id "}, {Name: ""}},
			"(`id`)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := g.formatIndexColumns(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
