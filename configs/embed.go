// Package configs embeds the default configuration files so every binary
// starts with a complete config even without a mounted file.
package configs

import "embed"

//go:embed *.yaml ig/*.yaml
var FS embed.FS

// Default returns the embedded default.yaml.
func Default() []byte {
	b, err := FS.ReadFile("default.yaml")
	if err != nil {
		panic(err)
	}
	return b
}
