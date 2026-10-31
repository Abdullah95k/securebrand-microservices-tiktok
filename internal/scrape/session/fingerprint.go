package session

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Fingerprint is a consistent browser identity: the tls-client profile and
// the headers that must agree with it.
type Fingerprint struct {
	Profile         string `yaml:"-"`
	UserAgent       string `yaml:"user_agent"`
	SecChUa         string `yaml:"sec_ch_ua"`
	SecChUaMobile   string `yaml:"sec_ch_ua_mobile"`
	SecChUaPlatform string `yaml:"sec_ch_ua_platform"`
}

// LoadFingerprints parses configs/fingerprints.yaml.
func LoadFingerprints(b []byte) (map[string]Fingerprint, error) {
	var doc struct {
		Profiles map[string]Fingerprint `yaml:"profiles"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("session: fingerprints: %w", err)
	}
	for k, v := range doc.Profiles {
		v.Profile = k
		doc.Profiles[k] = v
	}
	return doc.Profiles, nil
}
