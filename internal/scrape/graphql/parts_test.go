package graphql

import "testing"

func TestHasData(t *testing.T) {
	for body, want := range map[string]bool{
		`{"data":{"node":{"id":"1"}}}`:                                    true,
		`{"data":{"node":null}}`:                                          false,
		`{"data":{}}`:                                                     false,
		`{"data":null,"errors":[{"message":"x"}]}`:                        false,
		`{"errors":[{"code":1675004}]}`:                                   false,
		"{\"data\":{\"node\":null}}\n{\"data\":{\"feedback\":{}}}":        true,
		"for (;;);{\"data\":{\"a\":null,\"b\":1}}":                        true,
		`{"label":"x$defer","data":null,"errors":[{"severity":"ERROR"}]}`: false,
	} {
		if got := HasData(SplitParts([]byte(body))); got != want {
			t.Errorf("%s: got %v, want %v", body, got, want)
		}
	}
}
