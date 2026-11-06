// Package config loads layered configuration: embedded defaults, an optional
// YAML file, then environment variables (PREFIX_SECTION__KEY=value).
package config

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
)

// Load merges defaults, the files in paths (comma separated, later files
// win; e.g. a ConfigMap then a Secret) and environment variables starting
// with envPrefix into out. List values in env vars are comma separated; a
// value is split only where the key it sets is a list, so a password or a
// certificate with a comma reaches a string key whole.
func Load(defaults []byte, paths, envPrefix string, out any) (*koanf.Koanf, error) {
	return LoadLayers([][]byte{defaults}, paths, envPrefix, out)
}

// LoadLayers is Load with several default documents, merged in order
// (later ones win) before the files and environment variables.
func LoadLayers(defaults [][]byte, paths, envPrefix string, out any) (*koanf.Koanf, error) {
	k := koanf.New(".")
	for i, d := range defaults {
		if err := k.Load(rawbytes.Provider(d), yaml.Parser()); err != nil {
			return nil, fmt.Errorf("config: defaults %d: %w", i, err)
		}
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
			return key, value
		},
	})
	if err := k.Load(envProv, nil); err != nil {
		return nil, fmt.Errorf("config: env: %w", err)
	}
	dc := &mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			durationDays,
			mapstructure.StringToTimeDurationHookFunc(),
			splitList,
			mapstructure.TextUnmarshallerHookFunc()),
		WeaklyTypedInput: true,
		Result:           out,
	}
	if err := k.UnmarshalWithConf("", out, koanf.UnmarshalConf{DecoderConfig: dc}); err != nil {
		return nil, fmt.Errorf("config: unmarshal: %w", err)
	}
	return k, nil
}

// splitList turns a comma-separated string (an environment variable) into
// a list where the target is one: "a, b" becomes ["a", "b"].
func splitList(from, to reflect.Type, data any) (any, error) {
	if from.Kind() != reflect.String || to.Kind() != reflect.Slice {
		return data, nil
	}
	s := data.(string)
	if s == "" {
		return []string{}, nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts, nil
}

var dayDuration = regexp.MustCompile(`(\d+(?:\.\d+)?)d`)

// durationDays reads days in a duration ("7d", "1d12h"), as koanf's own
// decoder does.
func durationDays(from, to reflect.Type, data any) (any, error) {
	if from.Kind() != reflect.String || to != reflect.TypeOf(time.Duration(0)) {
		return data, nil
	}
	return dayDuration.ReplaceAllStringFunc(data.(string), func(d string) string {
		n, _ := strconv.ParseFloat(strings.TrimSuffix(d, "d"), 64)
		return strconv.FormatFloat(n*24, 'f', -1, 64) + "h"
	}), nil
}
