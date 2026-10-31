package walk

import "testing"

func TestParseCount(t *testing.T) {
	for in, want := range map[string]int64{
		"1,234 followers": 1234, "1.2M followers": 1_200_000, "980K likes": 980_000,
		"3,4K": 3400, "12": 12, "2.5B": 2_500_000_000,
	} {
		got, ok := ParseCount(in)
		if !ok || got != want {
			t.Errorf("ParseCount(%q) = %d, %v; want %d", in, got, ok, want)
		}
	}
}

func TestURLExpiry(t *testing.T) {
	ts := URLExpiry("https://scontent.xx.fbcdn.net/v/x.jpg?stp=dst&oe=6A1B2C3D&oh=00_abc")
	if ts == nil || ts.AsTime().Unix() != 0x6A1B2C3D {
		t.Fatalf("expiry = %v", ts)
	}
	if URLExpiry("https://example.com/x.jpg") != nil {
		t.Fatal("no oe should give nil")
	}
}
