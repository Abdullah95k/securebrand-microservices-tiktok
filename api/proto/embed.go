// Package protosrc embeds the public protobuf contract (securebrand.fb.v1)
// so `fbctl schemas apply` can register it with a Schema Registry.
package protosrc

import "embed"

// FS holds securebrand/fb/v1/*.proto at their import paths.
//
//go:embed securebrand/fb/v1/*.proto
var FS embed.FS
