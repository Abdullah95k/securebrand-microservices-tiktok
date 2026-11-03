package browser

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Forwarder is a proxy on a loopback port that a browser uses in place of
// an identity's exit: it forwards every request to the exit with the
// exit's credentials (or straight out when there is no exit), so the
// credentials never appear on a browser's command line, and counts the
// bytes it moves, which is what the exit bills.
type Forwarder struct {
	// URL is the proxy to give the browser (http://127.0.0.1:port).
	URL *url.URL

	up    *url.URL
	auth  string
	ln    net.Listener
	srv   *http.Server
	plain *http.Transport
	bytes atomic.Int64

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// NewForwarder starts a forwarder to upstream, an HTTP proxy URL with or
// without credentials; nil forwards straight out.
func NewForwarder(upstream *url.URL) (*Forwarder, error) {
	f := &Forwarder{conns: map[net.Conn]struct{}{}}
	if upstream != nil {
		if upstream.Scheme != "http" {
			return nil, fmt.Errorf("browser: forwarder: %s exits are not supported (HTTP proxies only)", upstream.Scheme)
		}
		up := *upstream
		if up.User != nil {
			pass, _ := up.User.Password()
			f.auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(up.User.Username()+":"+pass))
		}
		f.up = &up
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	f.ln = ln
	f.URL = &url.URL{Scheme: "http", Host: ln.Addr().String()}
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	f.plain = &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &countingConn{Conn: c, n: &f.bytes}, nil
		},
		MaxIdleConns: 4, IdleConnTimeout: 30 * time.Second, DisableCompression: true,
	}
	if f.up != nil {
		f.plain.Proxy = http.ProxyURL(f.up) // with its credentials, as the exit wants them
	}
	f.srv = &http.Server{Handler: http.HandlerFunc(f.serve), ReadHeaderTimeout: 30 * time.Second}
	go func() { _ = f.srv.Serve(ln) }()
	return f, nil
}

// Bytes is what the forwarder moved to and from the exit (or the sites,
// without one) so far.
func (f *Forwarder) Bytes() int64 { return f.bytes.Load() }

// Close stops the forwarder and its tunnels.
func (f *Forwarder) Close() error {
	err := f.srv.Close()
	f.mu.Lock()
	for c := range f.conns {
		_ = c.Close()
	}
	f.mu.Unlock()
	f.plain.CloseIdleConnections()
	return err
}

func (f *Forwarder) track(c net.Conn, on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if on {
		f.conns[c] = struct{}{}
	} else {
		delete(f.conns, c)
	}
}

func (f *Forwarder) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		f.tunnel(w, r)
		return
	}
	if !r.URL.IsAbs() {
		http.Error(w, "forwarder: proxy requests only", http.StatusBadRequest)
		return
	}
	out := r.Clone(r.Context())
	out.RequestURI = ""
	for _, h := range []string{"Proxy-Connection", "Proxy-Authorization", "Connection", "Keep-Alive", "Te", "Trailer", "Upgrade"} {
		out.Header.Del(h)
	}
	resp, err := f.plain.RoundTrip(out)
	if err != nil {
		http.Error(w, "forwarder: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// tunnel answers a CONNECT: it opens the same tunnel through the exit (or
// a connection to the site), then splices the two.
func (f *Forwarder) tunnel(w http.ResponseWriter, r *http.Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "forwarder: no hijack", http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	up, rd, err := f.dialTunnel(ctx, r.Host)
	if err != nil {
		http.Error(w, "forwarder: "+err.Error(), http.StatusBadGateway)
		return
	}
	client, buf, err := hj.Hijack()
	if err != nil {
		_ = up.Close()
		return
	}
	f.track(client, true)
	f.track(up, true)
	defer func() {
		f.track(client, false)
		f.track(up, false)
		_ = client.Close()
		_ = up.Close()
	}()
	if _, err := client.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n")); err != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() {
		// What the browser already sent past its CONNECT, then the rest.
		if n := buf.Reader.Buffered(); n > 0 {
			b, _ := buf.Peek(n)
			_, _ = up.Write(b)
		}
		_, _ = io.Copy(up, client)
		closeWrite(up)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, rd)
		closeWrite(client)
		done <- struct{}{}
	}()
	<-done
	<-done
}

// dialTunnel opens a byte stream to host:port: a CONNECT through the exit,
// or a plain connection without one. The reader holds what the exit sent
// after its answer.
func (f *Forwarder) dialTunnel(ctx context.Context, hostport string) (net.Conn, io.Reader, error) {
	var d net.Dialer
	if f.up == nil {
		c, err := d.DialContext(ctx, "tcp", hostport)
		if err != nil {
			return nil, nil, err
		}
		cc := &countingConn{Conn: c, n: &f.bytes}
		return cc, cc, nil
	}
	c, err := d.DialContext(ctx, "tcp", f.up.Host)
	if err != nil {
		return nil, nil, err
	}
	cc := &countingConn{Conn: c, n: &f.bytes}
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	req := "CONNECT " + hostport + " HTTP/1.1\r\nHost: " + hostport + "\r\n"
	if f.auth != "" {
		req += "Proxy-Authorization: " + f.auth + "\r\n"
	}
	if _, err := cc.Write([]byte(req + "\r\n")); err != nil {
		_ = c.Close()
		return nil, nil, err
	}
	rd := bufio.NewReader(cc)
	resp, err := http.ReadResponse(rd, &http.Request{Method: http.MethodConnect})
	if err != nil {
		_ = c.Close()
		return nil, nil, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_ = c.Close()
		// The status only: the exit's answer may echo the request.
		return nil, nil, errors.New("exit refused the tunnel: " + strings.TrimSpace(resp.Status))
	}
	_ = c.SetDeadline(time.Time{})
	return cc, rd, nil
}

func closeWrite(c net.Conn) {
	if cc, ok := c.(*countingConn); ok {
		c = cc.Conn
	}
	if tc, ok := c.(interface{ CloseWrite() error }); ok {
		_ = tc.CloseWrite()
	}
}

// countingConn adds the bytes read and written to n.
type countingConn struct {
	net.Conn
	n *atomic.Int64
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.n.Add(int64(n))
	return n, err
}

func (c *countingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.n.Add(int64(n))
	return n, err
}
