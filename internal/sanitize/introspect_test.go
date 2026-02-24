package sanitize

import "testing"

var identifierTests = []struct {
	name     string
	input    string
	expected string
}{
	{name: "empty string", input: "", expected: ""},
	{name: "simple valid identifier", input: "foo", expected: "foo"},
	{name: "uppercase letters", input: "FooBar", expected: "FooBar"},
	{name: "underscore prefix", input: "_foo", expected: "_foo"},
	{name: "alphanumeric", input: "foo123", expected: "foo123"},
	{name: "with dollar sign", input: "foo$bar", expected: "foo$bar"},
	{name: "replaces spaces with underscore", input: "foo bar", expected: "foo_bar"},
	{name: "replaces special chars with underscore", input: "foo-bar", expected: "foo_bar"},
	{name: "replaces multiple spaces with underscores", input: "foo bar baz", expected: "foo_bar_baz"},
	{name: "starts with digit", input: "123foo", expected: "_23foo"},
	{name: "starts with special char", input: "#foo", expected: "_foo"},
	{name: "trims whitespace", input: "  foo  ", expected: "foo"},
	{name: "trims and processes", input: "  foo bar  ", expected: "foo_bar"},
	{name: "all special chars become underscores", input: "!@#$%", expected: "___$_"},
	{name: "mixed valid and invalid", input: "foo_bar!baz123", expected: "foo_bar_baz123"},
}

func TestIdentifier(t *testing.T) {
	for _, tt := range identifierTests {
		t.Run(tt.name, func(t *testing.T) {
			result := Identifier(tt.input)
			if result != tt.expected {
				t.Errorf("Identifier(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

var identifierCharTests = []struct {
	name     string
	r        rune
	isFirst  bool
	expected bool
}{
	{name: "lowercase letter first", r: 'a', isFirst: true, expected: true},
	{name: "uppercase letter first", r: 'A', isFirst: true, expected: true},
	{name: "underscore first", r: '_', isFirst: true, expected: true},
	{name: "digit first - invalid", r: '0', isFirst: true, expected: false},
	{name: "dollar sign first - invalid", r: '$', isFirst: true, expected: false},
	{name: "special char first - invalid", r: '#', isFirst: true, expected: false},
	{name: "lowercase letter not first", r: 'a', isFirst: false, expected: true},
	{name: "uppercase letter not first", r: 'A', isFirst: false, expected: true},
	{name: "underscore not first", r: '_', isFirst: false, expected: true},
	{name: "digit not first", r: '0', isFirst: false, expected: true},
	{name: "dollar sign not first", r: '$', isFirst: false, expected: true},
	{name: "special char not first - invalid", r: '#', isFirst: false, expected: false},
	{name: "hyphen not first - invalid", r: '-', isFirst: false, expected: false},
	{name: "space not first - invalid", r: ' ', isFirst: false, expected: false},
}

func TestIdentifierChar(t *testing.T) {
	for _, tt := range identifierCharTests {
		t.Run(tt.name, func(t *testing.T) {
			result := IdentifierChar(tt.r, tt.isFirst)
			if result != tt.expected {
				t.Errorf("IdentifierChar(%q, %v) = %v, want %v", string(tt.r), tt.isFirst, result, tt.expected)
			}
		})
	}
}
