package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"smf/internal/validate"
)

func TestParseCheckConstraints(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantCount int
		wantNames []string
		wantExprs []string
	}{
		{
			name:      "single check constraint",
			input:     "CREATE TABLE `users` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `age` int DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  CONSTRAINT `chk_age` CHECK ((`age` > 0))\n) ENGINE=InnoDB",
			wantCount: 1,
			wantNames: []string{"chk_age"},
			wantExprs: []string{"(`age` > 0)"},
		},
		{
			name:      "multiple check constraints",
			input:     "CREATE TABLE `users` (\n  `id` int,\n  `age` int,\n  CONSTRAINT `chk_age` CHECK ((`age` > 0)),\n  CONSTRAINT `chk_id` CHECK ((`id` > 0))\n) ENGINE=InnoDB",
			wantCount: 2,
			wantNames: []string{"chk_age", "chk_id"},
			wantExprs: []string{"(`age` > 0)", "(`id` > 0)"},
		},
		{
			name:      "inline check constraint",
			input:     "CREATE TABLE `users` (\n  `id` int,\n  `age` int CHECK ((`age` >= 0))\n) ENGINE=InnoDB",
			wantCount: 0,
			wantNames: []string{},
			wantExprs: []string{},
		},
		{
			name:      "no check constraints",
			input:     "CREATE TABLE `users` (\n  `id` int PRIMARY KEY,\n  `name` varchar(255)\n) ENGINE=InnoDB",
			wantCount: 0,
			wantNames: []string{},
			wantExprs: []string{},
		},
		{
			name:      "check with complex expression",
			input:     "CREATE TABLE `users` (\n  `id` int,\n  `age` int,\n  CONSTRAINT `chk_age_valid` CHECK (((`age` is null) or (`age` between 0 and 150)))\n) ENGINE=InnoDB",
			wantCount: 1,
			wantNames: []string{"chk_age_valid"},
			wantExprs: []string{"((`age` is null) or (`age` between 0 and 150))"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseCheckConstraints(tt.input)
			assert.Len(t, got, tt.wantCount)

			for i, name := range tt.wantNames {
				expr, ok := got[name]
				assert.True(t, ok, "expected constraint %s to exist", name)
				if ok && i < len(tt.wantExprs) {
					assert.Equal(t, tt.wantExprs[i], expr)
				}
			}
		})
	}
}

func TestCountParens(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{"empty", "", 0},
		{"balanced", "(a + b)", 0},
		{"unbalanced open", "(a + b", 1},
		{"unbalanced close", "a + b)", -1},
		{"nested", "((a + b) * c)", 0},
		{"complex", "((`age` is null) or (`age` between 0 and 150))", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validate.CountParens(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseConstraintType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantName string
	}{
		{"primary key", "PRIMARY KEY", "PRIMARY KEY"},
		{"primary key lowercase", "primary key", "PRIMARY KEY"},
		{"foreign key", "FOREIGN KEY", "FOREIGN KEY"},
		{"unique", "UNIQUE", "UNIQUE"},
		{"check", "CHECK", "CHECK"},
		{"check lowercase", "check", "CHECK"},
		{"unknown type", "UNKNOWN", "CHECK"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseConstraintType(tt.input)
			assert.Equal(t, tt.wantName, string(got))
		})
	}
}

func TestParseReferentialAction(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantName string
	}{
		{"cascade", "CASCADE", "CASCADE"},
		{"cascade lowercase", "cascade", "CASCADE"},
		{"restrict", "RESTRICT", "RESTRICT"},
		{"set null", "SET NULL", "SET NULL"},
		{"set default", "SET DEFAULT", "SET DEFAULT"},
		{"no action", "NO ACTION", "NO ACTION"},
		{"empty", "", ""},
		{"unknown", "UNKNOWN", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseReferentialAction(tt.input)
			assert.Equal(t, tt.wantName, string(got))
		})
	}
}

func TestNormalizeIndexType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantName string
	}{
		{"btree", "BTREE", "BTREE"},
		{"btree lowercase", "btree", "BTREE"},
		{"hash", "HASH", "HASH"},
		{"fulltext", "FULLTEXT", "FULLTEXT"},
		{"spatial", "SPATIAL", "SPATIAL"},
		{"unknown", "UNKNOWN", "BTREE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeIndexType(tt.input)
			assert.Equal(t, tt.wantName, string(got))
		})
	}
}
