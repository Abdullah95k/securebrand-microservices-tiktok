package e2e

import (
	"bytes"
	"io"
)

type bytesReader struct{ b []byte }

func (r *bytesReader) reader() io.Reader {
	if r == nil {
		return nil
	}
	return bytes.NewReader(r.b)
}
