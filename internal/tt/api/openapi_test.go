package api

import (
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
)

// TestRoutesMatchOpenAPI keeps api/openapi/tks.v1.yaml and the router in
// step: every route is in the spec, every path and method of the spec has
// a route.
func TestRoutesMatchOpenAPI(t *testing.T) {
	b, err := os.ReadFile("../../../api/openapi/tks.v1.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(b, &spec); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for p, ops := range spec.Paths {
		for m := range ops {
			if m == "parameters" {
				continue
			}
			want[strings.ToUpper(m)+" /v1"+p] = true
		}
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	r := (&Server{Cfg: cfg}).Handler().(chi.Routes)
	got := map[string]bool{}
	_ = chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		got[method+" "+strings.TrimSuffix(route, "/")] = true
		return nil
	})
	var missing, extra []string
	for k := range got {
		if !want[k] {
			missing = append(missing, k)
		}
	}
	for k := range want {
		if !got[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("routes not in the OpenAPI spec: %v\nspec paths without a route: %v", missing, extra)
	}
	if len(got) < 20 {
		t.Fatalf("only %d routes walked", len(got))
	}
}
