// Package config loads layered configuration: embedded defaults, an optional
// YAML file, then environment variables (PREFIX_SECTION__KEY=value).
package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
)

// Load merges defaults, the files in paths (comma separated, later files
// win; e.g. a ConfigMap then a Secret) and environment variables starting
// with envPrefix into out. List values in env vars are comma separated.
func Load(defaults []byte, paths, envPrefix string, out any) (*koanf.Koanf, error) {
	k := koanf.New(".")
	if err := k.Load(rawbytes.Provider(defaults), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("config: defaults: %w", err)
	}
	for _, path := range strings.Split(paths, ",") {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("config: %w", err)
		}
		if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
			return nil, fmt.Errorf("config: %s: %w", path, err)
		}
	}
	envProv := env.Provider(".", env.Opt{
		Prefix: envPrefix,
		TransformFunc: func(key, value string) (string, any) {
			key = strings.ToLower(strings.TrimPrefix(key, envPrefix))
			key = strings.ReplaceAll(key, "__", ".")
			if strings.Contains(value, ",") {
				parts := strings.Split(value, ",")
				for i := range parts {
					parts[i] = strings.TrimSpace(parts[i])
				}
				return key, parts
			}
			return key, value
		},
	})
	if err := k.Load(envProv, nil); err != nil {
		return nil, fmt.Errorf("config: env: %w", err)
	}
	if err := k.Unmarshal("", out); err != nil {
		return nil, fmt.Errorf("config: unmarshal: %w", err)
	}
	return k, nil
}
