package s3x

import "testing"

func TestEndpoint(t *testing.T) {
	for in, want := range map[string]struct {
		host string
		ssl  bool
	}{
		"https://fra1.example.com": {"fra1.example.com", true},
		"http://minio:9000/":       {"minio:9000", false},
		"localhost:9000":           {"localhost:9000", false},
		"fra1.example.com/":        {"fra1.example.com", false},
	} {
		h, ssl := Endpoint(in, false)
		if h != want.host || ssl != want.ssl {
			t.Errorf("%s: %s %v", in, h, ssl)
		}
	}
}
