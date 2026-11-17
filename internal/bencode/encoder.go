package bencode

import (
	"bytes"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
)

// MarshalValue writes the canonical bencoding of v to w.
//
// Supported types are int, int64, string, []byte, []any and map[string]any.
// Dictionary keys are emitted sorted by their raw bytes, as BEP 3 requires, so
// the same value always encodes to the same bytes.
func MarshalValue(w io.Writer, v any) error {
	buf, err := appendValue(nil, v)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// Marshal returns the canonical bencoding of v.
func Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := MarshalValue(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func appendValue(dst []byte, v any) ([]byte, error) {
	switch v := v.(type) {
	case int:
		return appendInt(dst, int64(v)), nil
	case int64:
		return appendInt(dst, v), nil
	case string:
		return appendString(dst, v), nil
	case []byte:
		dst = strconv.AppendInt(dst, int64(len(v)), 10)
		dst = append(dst, ':')
		return append(dst, v...), nil
	case []any:
		dst = append(dst, 'l')
		for _, item := range v {
			var err error
			if dst, err = appendValue(dst, item); err != nil {
				return nil, err
			}
		}
		return append(dst, 'e'), nil
	case map[string]any:
		dst = append(dst, 'd')
		// Go string comparison is bytewise, which matches BEP 3's
		// "sorted as raw strings" rule.
		for _, key := range slices.Sorted(maps.Keys(v)) {
			dst = appendString(dst, key)
			var err error
			if dst, err = appendValue(dst, v[key]); err != nil {
				return nil, err
			}
		}
		return append(dst, 'e'), nil
	default:
		return nil, fmt.Errorf("bencode: unsupported type %T", v)
	}
}

func appendInt(dst []byte, n int64) []byte {
	dst = append(dst, 'i')
	dst = strconv.AppendInt(dst, n, 10)
	return append(dst, 'e')
}

func appendString(dst []byte, s string) []byte {
	dst = strconv.AppendInt(dst, int64(len(s)), 10)
	dst = append(dst, ':')
	return append(dst, s...)
}
