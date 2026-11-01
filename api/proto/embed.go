// Package protosrc embeds the public protobuf contracts so `fbctl schemas
// apply` (securebrand.fb.v1) and `tksctl schemas apply` (securebrand.tt.v1
// and securebrand.common.v1) can register them with a Schema Registry.
package protosrc

import "embed"

// FS holds the public proto files at their import paths. The internal task
// envelopes (fbs.task.v1, tks.task.v1) are not registered.
//
//go:embed securebrand/fb/v1/*.proto securebrand/common/v1/*.proto securebrand/tt/v1/*.proto
var FS embed.FS
