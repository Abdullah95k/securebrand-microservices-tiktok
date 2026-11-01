// Package tt embeds tks's goose migrations for schema tt (docs/tiktok ADR
// 0007 §4). They run with search_path pinned to tt and keep their version
// table at tt.goose_db_version (internal/tt/ttstore), apart from the copied
// Facebook migrations in the parent directory.
package tt

import "embed"

//go:embed *.sql
var FS embed.FS
