package bencode

import (
	"errors"
	"fmt"
	"strconv"
)

// MaxDepth is the deepest list/dictionary nesting Decode accepts. Real
// torrents, tracker replies and extension messages nest only a few levels;
// the limit stops crafted input from exhausting the goroutine stack.
const MaxDepth = 64

// Errors returned for malformed input. Callers can match them with errors.Is.
var (
	ErrSyntax       = errors.New("bencode: syntax error")
	ErrNonCanonical = errors.New("bencode: non-canonical encoding")
	ErrTooDeep      = errors.New("bencode: nesting too deep")
)

// Decode parses a single bencoded value from data and returns:
//   - the decoded value (int64 for integers, string for strings, []any for
//     lists, map[string]any for dictionaries)
//   - the remaining unconsumed bytes
//   - any error encountered
//
// Input is treated as untrusted: lengths are checked without overflow,
// non-canonical integers and string lengths are rejected, and nesting is
// limited to MaxDepth.
func Decode(data []byte) (any, []byte, error) {
	return decode(data, 0)
}

func decode(data []byte, depth int) (any, []byte, error) {
	if len(data) == 0 {
		return nil, data, fmt.Errorf("%w: empty input", ErrSyntax)
	}
	switch c := data[0]; {
	case c == 'i':
		return decodeInteger(data)
	case c >= '0' && c <= '9':
		return decodeString(data)
	case c == 'l':
		return decodeList(data, depth+1)
	case c == 'd':
		return decodeDict(data, depth+1)
	default:
		return nil, data, fmt.Errorf("%w: unknown type %q", ErrSyntax, c)
	}
}

// decodeInteger decodes a bencode integer in the form i<number>e.
func decodeInteger(data []byte) (any, []byte, error) {
	end := 1
	for end < len(data) && data[end] != 'e' {
		end++
	}
	if end >= len(data) {
		return nil, data, fmt.Errorf("%w: integer missing 'e'", ErrSyntax)
	}

	num := data[1:end]
	if err := checkCanonicalInteger(num); err != nil {
		return nil, data, err
	}
	n, err := strconv.ParseInt(string(num), 10, 64)
	if err != nil {
		return nil, data, fmt.Errorf("%w: invalid integer %q", ErrSyntax, num)
	}
	return n, data[end+1:], nil
}

// checkCanonicalInteger enforces BEP 3: digits only with an optional leading
// '-', no leading zeros, and no negative zero.
func checkCanonicalInteger(num []byte) error {
	digits := num
	if len(digits) > 0 && digits[0] == '-' {
		digits = digits[1:]
		if len(digits) > 0 && digits[0] == '0' {
			return fmt.Errorf("%w: integer %q", ErrNonCanonical, num)
		}
	}
	if len(digits) == 0 {
		return fmt.Errorf("%w: empty integer", ErrSyntax)
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return fmt.Errorf("%w: invalid integer %q", ErrSyntax, num)
		}
	}
	if len(digits) > 1 && digits[0] == '0' {
		return fmt.Errorf("%w: integer %q", ErrNonCanonical, num)
	}
	return nil
}

// decodeString decodes a bencode string in the form <length>:<content>.
func decodeString(data []byte) (any, []byte, error) {
	colon := 0
	for colon < len(data) && data[colon] != ':' {
		if data[colon] < '0' || data[colon] > '9' {
			return nil, data, fmt.Errorf("%w: invalid string length", ErrSyntax)
		}
		colon++
	}
	if colon >= len(data) {
		return nil, data, fmt.Errorf("%w: string missing ':'", ErrSyntax)
	}
	if colon > 1 && data[0] == '0' {
		return nil, data, fmt.Errorf("%w: string length %q", ErrNonCanonical, data[:colon])
	}

	length, err := strconv.Atoi(string(data[:colon]))
	if err != nil {
		return nil, data, fmt.Errorf("%w: invalid string length %q", ErrSyntax, data[:colon])
	}

	// Compare against the remaining input instead of computing start+length,
	// which can overflow for a crafted length.
	start := colon + 1
	if length > len(data)-start {
		return nil, data, fmt.Errorf("%w: string length %d exceeds %d remaining bytes", ErrSyntax, length, len(data)-start)
	}
	end := start + length
	return string(data[start:end]), data[end:], nil
}

// decodeList decodes a bencode list in the form l<items>e.
func decodeList(data []byte, depth int) (any, []byte, error) {
	if depth > MaxDepth {
		return nil, data, ErrTooDeep
	}

	list := make([]any, 0)
	rest := data[1:]
	for {
		if len(rest) == 0 {
			return nil, data, fmt.Errorf("%w: list missing 'e'", ErrSyntax)
		}
		if rest[0] == 'e' {
			return list, rest[1:], nil
		}
		item, next, err := decode(rest, depth)
		if err != nil {
			return nil, data, err
		}
		list = append(list, item)
		rest = next
	}
}

// decodeDict decodes a bencode dictionary in the form d<items>e.
func decodeDict(data []byte, depth int) (any, []byte, error) {
	if depth > MaxDepth {
		return nil, data, ErrTooDeep
	}

	dict := make(map[string]any)
	rest := data[1:]
	for {
		if len(rest) == 0 {
			return nil, data, fmt.Errorf("%w: dict missing 'e'", ErrSyntax)
		}
		if rest[0] == 'e' {
			return dict, rest[1:], nil
		}

		keyVal, next, err := decodeString(rest)
		if err != nil {
			return nil, data, fmt.Errorf("dict key: %w", err)
		}
		key := keyVal.(string)
		if _, duplicate := dict[key]; duplicate {
			return nil, data, fmt.Errorf("%w: duplicate dict key %q", ErrNonCanonical, key)
		}

		value, next, err := decode(next, depth)
		if err != nil {
			return nil, data, err
		}
		dict[key] = value
		rest = next
	}
}
