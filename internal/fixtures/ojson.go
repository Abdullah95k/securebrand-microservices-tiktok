package fixtures

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// object is a JSON object that keeps its keys in the order written, so a
// scrubbed fixture keeps the layout that order-sensitive patterns (the
// page id patterns in htmlx) rely on.
type object struct {
	keys []string
	vals []any
}

func (o *object) get(k string) any {
	for i, key := range o.keys {
		if key == k {
			return o.vals[i]
		}
	}
	return nil
}

// decodeAll reads every JSON value in r, keeping key order and numbers as
// written (objects become *object, arrays []any, numbers json.Number).
func decodeAll(r io.Reader) ([]any, error) {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	var out []any
	for {
		v, err := decodeValue(dec, true)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
}

func decodeValue(dec *json.Decoder, top bool) (any, error) {
	tok, err := dec.Token()
	if !top {
		err = unexpected(err)
	}
	if err != nil {
		return nil, err
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return tok, nil
	}
	switch d {
	case '{':
		o := &object{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return nil, unexpected(err)
			}
			k, _ := kt.(string)
			v, err := decodeValue(dec, false)
			if err != nil {
				return nil, err
			}
			o.keys, o.vals = append(o.keys, k), append(o.vals, v)
		}
		if _, err := dec.Token(); err != nil {
			return nil, unexpected(err)
		}
		return o, nil
	case '[':
		arr := []any{}
		for dec.More() {
			v, err := decodeValue(dec, false)
			if err != nil {
				return nil, err
			}
			arr = append(arr, v)
		}
		if _, err := dec.Token(); err != nil {
			return nil, unexpected(err)
		}
		return arr, nil
	}
	return nil, fmt.Errorf("fixtures: unexpected %v", d)
}

// unexpected turns the end of input inside a value into an error: only
// the end between values is the end of the stream.
func unexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// encode writes v as compact JSON; escapeHTML escapes <, > and & (needed
// inside a <script>).
func encode(v any, escapeHTML bool) []byte {
	e := &encoder{}
	e.enc = json.NewEncoder(&e.tmp)
	e.enc.SetEscapeHTML(escapeHTML)
	e.value(v)
	return e.buf.Bytes()
}

type encoder struct {
	buf, tmp bytes.Buffer
	enc      *json.Encoder
}

func (e *encoder) value(v any) {
	switch v := v.(type) {
	case *object:
		e.buf.WriteByte('{')
		for i, k := range v.keys {
			if i > 0 {
				e.buf.WriteByte(',')
			}
			e.scalar(k)
			e.buf.WriteByte(':')
			e.value(v.vals[i])
		}
		e.buf.WriteByte('}')
	case []any:
		e.buf.WriteByte('[')
		for i, x := range v {
			if i > 0 {
				e.buf.WriteByte(',')
			}
			e.value(x)
		}
		e.buf.WriteByte(']')
	default:
		e.scalar(v)
	}
}

func (e *encoder) scalar(v any) {
	e.tmp.Reset()
	_ = e.enc.Encode(v)
	e.buf.Write(bytes.TrimSuffix(e.tmp.Bytes(), []byte("\n")))
}
