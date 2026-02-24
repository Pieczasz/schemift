// Package pars provides the Parser interface for reading schema files
// in various formats (TOML, JSON, YAML, etc.) and converting them to
// the canonical core.Database representation.
package pars

import (
	"fmt"
	"io"
	"path/filepath"

	"smf/internal/core"
	"smf/internal/pars/toml"
)

type Parser interface {
	Parse(r io.Reader) (*core.Database, error)
}

func ParseFile(path string) (*core.Database, error) {
	ext := filepath.Ext(path)

	switch ext {
	case ".toml":
		return toml.NewParser().ParseFile(path)
	default:
		return nil, fmt.Errorf("unsupported file format: %v", path)
	}
}
