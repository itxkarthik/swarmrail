package bencode

import (
	"reflect"
	"testing"
)

// FuzzDecode checks that Decode never panics on untrusted input and that any
// value it accepts survives an encode/decode round trip unchanged.
func FuzzDecode(f *testing.F) {
	for _, seed := range []string{
		"i42e", "i-1e", "0:", "4:spam", "le", "de",
		"l4:spami42ee", "d3:cow3:moo4:spam4:eggse",
		"d8:announce3:url4:infod6:lengthi10e4:name1:a12:piece lengthi16384eee",
		"9223372036854775807:abc", "i-0e", "03:abc", "llllllllle",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		value, rest, err := Decode(data)
		if err != nil {
			return
		}
		if len(rest) > len(data) {
			t.Fatalf("rest is longer than the input")
		}
		encoded, err := Marshal(value)
		if err != nil {
			t.Fatalf("decoded value cannot be encoded: %v", err)
		}
		again, tail, err := Decode(encoded)
		if err != nil || len(tail) != 0 {
			t.Fatalf("re-decoding %q: %v (tail %q)", encoded, err, tail)
		}
		if !reflect.DeepEqual(value, again) {
			t.Fatalf("round trip changed the value: %#v != %#v", value, again)
		}
	})
}
