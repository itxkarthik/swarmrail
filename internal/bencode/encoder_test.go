package bencode

import "testing"

func TestMarshalString(t *testing.T) {
	got, err := Marshal("hello")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "5:hello" {
		t.Fatalf("expected %q, got %q", "5:hello", string(got))
	}
}

func TestMarshalEmptyString(t *testing.T) {
	got, err := Marshal("")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "0:" {
		t.Fatalf("expected %q, got %q", "0:", string(got))
	}
}

func TestMarshalIntegerTypes(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  string
	}{
		{name: "int", value: int(7), want: "i7e"},
		{name: "int64", value: int64(7), want: "i7e"},
		{name: "negative int", value: int(-5), want: "i-5e"},
		{name: "negative int64", value: int64(-5), want: "i-5e"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, string(got))
			}
		})
	}
}

func TestMarshalEmptyListAndDict(t *testing.T) {
	list, err := Marshal([]any{})
	if err != nil {
		t.Fatal(err)
	}
	if string(list) != "le" {
		t.Fatalf("expected %q, got %q", "le", string(list))
	}

	dict, err := Marshal(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if string(dict) != "de" {
		t.Fatalf("expected %q, got %q", "de", string(dict))
	}
}

func TestMarshalMapIntValues(t *testing.T) {
	got, err := Marshal(map[string]any{
		"size":  int64(9),
		"count": int(3),
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := "d5:counti3e4:sizei9ee"; string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMarshalSortsKeysByRawBytes(t *testing.T) {
	value := map[string]any{
		"z":       int64(1),
		"a":       int64(2),
		"B":       int64(3), // uppercase sorts before lowercase
		"\xff":    int64(4), // high bytes sort last
		"aa":      int64(5),
		"m":       map[string]any{"y": "1", "x": "2"},
		"ut_pex":  int64(2),
		"ut_meta": int64(1),
	}
	want := "d1:Bi3e1:ai2e2:aai5e1:md1:x1:21:y1:1e7:ut_metai1e6:ut_pexi2e1:zi1e1:\xffi4ee"
	for range 50 { // map order is randomised; every run must agree
		got, err := Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestMarshalRoundTripIsByteIdentical(t *testing.T) {
	inputs := []string{
		"d8:announce3:url4:infod6:lengthi10e4:name1:a12:piece lengthi16384eee",
		"l4:spami-3eli0eed0:0:ee",
		"d1:ad1:bd1:cleeee",
	}
	for _, input := range inputs {
		value, rest, err := Decode([]byte(input))
		if err != nil || len(rest) != 0 {
			t.Fatalf("Decode(%q): %v, rest %q", input, err, rest)
		}
		got, err := Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != input {
			t.Fatalf("round trip changed bytes:\n got  %q\n want %q", got, input)
		}
	}
}

func TestMarshalRejectsUnsupportedTypes(t *testing.T) {
	if _, err := Marshal(map[string]any{"bad": 1.5}); err == nil {
		t.Fatal("expected an error for float64")
	}
}
