package bencode

import (
	"errors"
	"strings"
	"testing"
)

func TestDecodeInteger(t *testing.T) {
	result, _, err := Decode([]byte("i42e"))
	if err != nil {
		t.Fatal(err)
	}
	if result != int64(42) {
		t.Fatalf("expected 42, got %v", result)
	}
}

func TestDecodeString(t *testing.T) {
	result, _, err := Decode([]byte("5:hello"))
	if err != nil {
		t.Fatal(err)
	}
	if result != "hello" {
		t.Fatalf("expected \"hello\", got %v", result)
	}
}

func TestDecodeEmptyString(t *testing.T) {
	result, _, err := Decode([]byte("0:"))
	if err != nil {
		t.Fatal(err)
	}
	if result != "" {
		t.Fatalf("expected empty string, got %v", result)
	}
}

func TestDecodeNegativeInteger(t *testing.T) {
	result, _, err := Decode([]byte("i-42e"))
	if err != nil {
		t.Fatal(err)
	}
	if result != int64(-42) {
		t.Fatalf("expected -42, got %v", result)
	}
}

func TestDecodeZeroInteger(t *testing.T) {
	result, _, err := Decode([]byte("i0e"))
	if err != nil {
		t.Fatal(err)
	}
	if result != int64(0) {
		t.Fatalf("expected 0, got %v", result)
	}
}

func TestDecodeInvalidInteger(t *testing.T) {
	_, _, err := Decode([]byte("i42"))
	if err == nil {
		t.Fatal("expected error for missing 'e'")
	}
}

func TestDecodeInvalidString(t *testing.T) {
	_, _, err := Decode([]byte("5:hi"))
	if err == nil {
		t.Fatal("expected error for short string")
	}
}

func TestDecodeEmptyInput(t *testing.T) {
	_, _, err := Decode([]byte(""))
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestDecodeUnknownType(t *testing.T) {
	_, _, err := Decode([]byte("x"))
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestDecodeRemainingBytes(t *testing.T) {
	val, rest, err := Decode([]byte("i42e5:hello"))
	if err != nil {
		t.Fatal(err)
	}
	if val != int64(42) {
		t.Fatalf("expected 42, got %v", val)
	}
	if string(rest) != "5:hello" {
		t.Fatalf("expected remaining \"5:hello\", got %q", string(rest))
	}
}

func TestDecodeRejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  error
	}{
		{"overflowing string length", "9223372036854775807:abc", ErrSyntax},
		{"length larger than input", "10:abc", ErrSyntax},
		{"leading zero length", "03:abc", ErrNonCanonical},
		{"non-digit length", "3a:abc", ErrSyntax},
		{"negative zero", "i-0e", ErrNonCanonical},
		{"leading zero integer", "i03e", ErrNonCanonical},
		{"negative leading zero", "i-03e", ErrNonCanonical},
		{"plus sign", "i+5e", ErrSyntax},
		{"empty integer", "ie", ErrSyntax},
		{"lone minus", "i-e", ErrSyntax},
		{"integer overflow", "i9223372036854775808e", ErrSyntax},
		{"unterminated list", "li1e", ErrSyntax},
		{"unterminated dict", "d1:ai1e", ErrSyntax},
		{"integer dict key", "di1ei2ee", ErrSyntax},
		{"duplicate dict key", "d1:ai1e1:ai2ee", ErrNonCanonical},
		{"dict key without value", "d1:ae", ErrSyntax},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Decode([]byte(tt.input))
			if !errors.Is(err, tt.want) {
				t.Fatalf("Decode(%q) error = %v, want %v", tt.input, err, tt.want)
			}
		})
	}
}

func TestDecodeAcceptsCanonicalEdgeCases(t *testing.T) {
	tests := []struct {
		input string
		want  any
	}{
		{"i0e", int64(0)},
		{"i-1e", int64(-1)},
		{"i9223372036854775807e", int64(9223372036854775807)},
		{"i-9223372036854775808e", int64(-9223372036854775808)},
		{"0:", ""},
		{"10:0123456789", "0123456789"},
	}
	for _, tt := range tests {
		got, rest, err := Decode([]byte(tt.input))
		if err != nil || got != tt.want || len(rest) != 0 {
			t.Fatalf("Decode(%q) = %v, %q, %v; want %v", tt.input, got, rest, err, tt.want)
		}
	}
}

func TestDecodeLimitsNesting(t *testing.T) {
	atLimit := strings.Repeat("l", MaxDepth) + strings.Repeat("e", MaxDepth)
	if _, _, err := Decode([]byte(atLimit)); err != nil {
		t.Fatalf("nesting at MaxDepth: %v", err)
	}
	tooDeep := strings.Repeat("l", MaxDepth+1) + strings.Repeat("e", MaxDepth+1)
	if _, _, err := Decode([]byte(tooDeep)); !errors.Is(err, ErrTooDeep) {
		t.Fatalf("nesting beyond MaxDepth: error = %v, want ErrTooDeep", err)
	}
	unbounded := strings.Repeat("d1:a", 100000)
	if _, _, err := Decode([]byte(unbounded)); !errors.Is(err, ErrTooDeep) {
		t.Fatalf("deep dict nesting: error = %v, want ErrTooDeep", err)
	}
}
