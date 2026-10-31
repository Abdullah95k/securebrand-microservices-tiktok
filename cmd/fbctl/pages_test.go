package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPagesImport(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k" || r.URL.Path != "/v1/pages" {
			http.Error(w, "nope", http.StatusUnauthorized)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		page, _ := body["page"].(string)
		mu.Lock()
		seen[page]++
		n := seen[page]
		mu.Unlock()
		switch {
		case page == "bad":
			http.Error(w, `{"title":"invalid page"}`, http.StatusBadRequest)
		case page == "busy" && n == 1:
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"page_ref": page, "created": page != "known"})
		}
	}))
	defer srv.Close()

	file := filepath.Join(t.TempDir(), "pages.csv")
	if err := os.WriteFile(file, []byte("page,size\nbrand1,small\nknown,large\nbusy,small\nbad,small\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := pagesImport(context.Background(), []string{"--api", srv.URL + "/", "--key", "k", "--rps", "50", file})
	if err == nil {
		t.Fatal("the invalid page must make the import fail")
	}
	if seen["busy"] != 2 || seen["brand1"] != 1 || seen["known"] != 1 || seen["bad"] != 1 {
		t.Fatalf("requests per page %v", seen)
	}

	im := importer{url: srv.URL + "/v1/pages", key: "k", http: srv.Client(), sleep: func(time.Duration) {}}
	if created, err := im.register(context.Background(), map[string]any{"page": "known"}); err != nil || created {
		t.Fatalf("known page: created=%v err=%v", created, err)
	}
	if created, err := im.register(context.Background(), map[string]any{"page": "new"}); err != nil || !created {
		t.Fatalf("new page: created=%v err=%v", created, err)
	}
}
