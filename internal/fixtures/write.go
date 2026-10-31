package fixtures

import (
	"os"
	"path/filepath"
	"time"
)

// Write stores a parser fixture as root/<query>/<caseName>/<file> with a
// meta.json. The body is scrubbed of personal data before it reaches the
// disk (keep: ids to leave as they are, such as the page id), so a failure
// can never leave the raw response behind.
func Write(root, query, caseName, file string, body []byte, meta map[string]any, keep []string) (string, error) {
	d := filepath.Join(root, query, caseName)
	if err := os.MkdirAll(d, 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(d, file), Scrub(body, keepSet(keep)), 0o644); err != nil { //nolint:gosec // fixtures are meant to be committed
		return "", err
	}
	if meta == nil {
		meta = map[string]any{}
	}
	meta["captured_at"] = time.Now().UTC()
	if err := writeMeta(d, meta); err != nil {
		return "", err
	}
	return d, nil
}
