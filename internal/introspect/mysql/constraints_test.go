package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		{"unknown type", "UNKNOWN", "UNKNOWN"},
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
		{"unknown", "UNKNOWN", "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeIndexType(tt.input)
			assert.Equal(t, tt.wantName, string(got))
		})
	}
}
