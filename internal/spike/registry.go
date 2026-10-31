package spike

import (
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

// DocIDResult is the outcome of merging discovered doc_ids into a seed.
type DocIDResult struct {
	Found   map[string]string `json:"found"`   // friendly_name -> doc_id, for names the seed uses
	Missing []string          `json:"missing"` // friendly names the seed uses that no source exposed
	Updated []string          `json:"updated"` // query names whose doc_id changed
}

// MergeDocIDs returns a copy of seed with the doc_ids in found applied by
// friendly name: every query sharing a friendly name (comments,
// comments_newest, ...) gets the same id.
func MergeDocIDs(seed registry.Seed, found map[string]string) (registry.Seed, DocIDResult) {
	res := DocIDResult{Found: map[string]string{}}
	out := registry.Seed{ClientParams: seed.ClientParams, Queries: append([]registry.Query(nil), seed.Queries...)}
	missing := map[string]bool{}
	for i, q := range out.Queries {
		id, ok := found[q.FriendlyName]
		if !ok || id == "" {
			missing[q.FriendlyName] = true
			continue
		}
		res.Found[q.FriendlyName] = id
		if q.DocID != id {
			out.Queries[i].DocID = id
			res.Updated = append(res.Updated, q.Name)
		}
	}
	for name := range missing {
		if _, ok := res.Found[name]; !ok {
			res.Missing = append(res.Missing, name)
		}
	}
	sort.Strings(res.Missing)
	return out, res
}

// WriteRegistry writes seed as a registry YAML file usable with --registry
// and as configs/queries.seed.yaml.
func WriteRegistry(path string, seed registry.Seed, source string) error {
	b, err := registry.MarshalSeed(seed)
	if err != nil {
		return err
	}
	head := fmt.Sprintf("# Registry written by fbs-spike on %s from %s.\n# Review it, then pass it with --registry or copy it to configs/queries.seed.yaml.\n",
		time.Now().UTC().Format(time.RFC3339), source)
	return os.WriteFile(path, append([]byte(head), b...), 0o644)
}

// ReadRegistry reads a registry YAML file.
func ReadRegistry(path string) (registry.Seed, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return registry.Seed{}, err
	}
	return registry.ParseSeed(b)
}

// IsFacebook reports whether baseURL points at real Facebook rather than
// fakefb.
func IsFacebook(baseURL string) bool {
	u, err := url.Parse(baseURL)
	if err != nil {
		return false
	}
	h := strings.ToLower(u.Hostname())
	return h == "facebook.com" || strings.HasSuffix(h, ".facebook.com")
}

// ErrPlaceholderDocIDs means the registry still has the seed placeholders,
// which real Facebook rejects: every request would be wasted.
var ErrPlaceholderDocIDs = fmt.Errorf("spike: registry has placeholder doc_ids; run fbs-spike docids (or har) first and pass the result with --registry")

// CheckRegistry refuses placeholder doc_ids against real Facebook.
func CheckRegistry(seed registry.Seed, baseURL string) error {
	if q := registry.PlaceholderQueries(seed); len(q) > 0 && IsFacebook(baseURL) {
		return fmt.Errorf("%w (%s)", ErrPlaceholderDocIDs, strings.Join(q, ", "))
	}
	return nil
}
