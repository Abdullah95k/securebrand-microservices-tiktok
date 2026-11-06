package app

import (
	"os"
	"regexp"
	"slices"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/persist"
)

// The chart's alerts and KEDA triggers name consumer groups and topics by
// string; a name that matches none of tks's makes an alert that never fires
// or an autoscaler that never scales (the alerts copied from fbs selected
// fbs's tks-crawler-* and tks-persister, which tks's groups are not).

const chart = "../../../deploy/helm/tks"

func groups() []string {
	out := []string{persist.Group, MediaDownloaderGroup, MediaLedgerGroup}
	for _, s := range Stages {
		if s == "retry" {
			continue
		}
		for _, l := range []string{"p0", "p1", "p2"} {
			out = append(out, "tks-tt-crawler-"+s+"-"+l)
		}
	}
	return out
}

func TestConsumerGroupNames(t *testing.T) {
	// groups() spells ConsumerGroup out, so a rename there fails here.
	for _, s := range []string{"account", "post", "thread"} {
		if g := ConsumerGroup(s, 0); !slices.Contains(groups(), g) {
			t.Fatalf("ConsumerGroup(%s) = %s, not in %v", s, g, groups())
		}
	}
}

func TestAlertGroupSelectors(t *testing.T) {
	b, err := os.ReadFile(chart + "/files/alerts.yaml")
	if err != nil {
		t.Fatal(err)
	}
	sel := regexp.MustCompile(`group(=~|=)"([^"]+)"`)
	found := 0
	for _, m := range sel.FindAllStringSubmatch(string(b), -1) {
		found++
		re := regexp.MustCompile("^" + regexp.QuoteMeta(m[2]) + "$")
		if m[1] == "=~" {
			re = regexp.MustCompile("^(?:" + m[2] + ")$")
		}
		if !slices.ContainsFunc(groups(), re.MatchString) {
			t.Errorf("alerts select group%s%q: no tks consumer group matches (%v)", m[1], m[2], groups())
		}
	}
	if found == 0 {
		t.Fatal("no group selectors found in the alerts")
	}
}

func TestChartAutoscaleTargets(t *testing.T) {
	b, err := os.ReadFile(chart + "/values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Components map[string]struct {
			Command   []string `yaml:"command"`
			Autoscale *struct {
				Lag []struct{ Group, Topic string }
			} `yaml:"autoscale"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	tb, err := configs.FS.ReadFile("tt/topics.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var tf struct{ Topics []struct{ Name string } }
	if err := yaml.Unmarshal(tb, &tf); err != nil {
		t.Fatal(err)
	}
	var topicNames []string
	for _, tp := range tf.Topics {
		topicNames = append(topicNames, tp.Name)
	}
	scaled := 0
	for name, c := range v.Components {
		if len(c.Command) == 0 || !regexp.MustCompile(`^/usr/local/bin/tks-`).MatchString(c.Command[0]) {
			t.Errorf("component %s runs %v, not a tks binary", name, c.Command)
		}
		if c.Autoscale == nil {
			continue
		}
		for _, l := range c.Autoscale.Lag {
			scaled++
			if !slices.Contains(groups(), l.Group) {
				t.Errorf("%s scales on group %s: not a tks consumer group", name, l.Group)
			}
			if !slices.Contains(topicNames, l.Topic) {
				t.Errorf("%s scales on topic %s: not in configs/tt/topics.yaml", name, l.Topic)
			}
		}
	}
	if scaled == 0 {
		t.Fatal("no autoscale triggers found")
	}
}

// Only tks-signer runs Chromium (M9d), in a pod whose memory request is
// its limit, at one CPU at most; the crawlers that send signed calls and
// the canary run the default image within 1 GiB.
func TestChartSignerIsCapped(t *testing.T) {
	b, err := os.ReadFile(chart + "/values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Components map[string]struct {
			Browser   bool     `yaml:"browser"`
			Service   bool     `yaml:"service"`
			Port      int      `yaml:"port"`
			Command   []string `yaml:"command"`
			Resources struct {
				Requests map[string]string `yaml:"requests"`
				Limits   map[string]string `yaml:"limits"`
			} `yaml:"resources"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	for name, c := range v.Components {
		if c.Browser != (name == "signer") {
			t.Errorf("%s: browser image %v; only tks-signer runs Chromium", name, c.Browser)
		}
	}
	s := v.Components["signer"]
	if !slices.Equal(s.Command, []string{"/usr/local/bin/tks-signer"}) || !s.Service || s.Port != 8080 {
		t.Errorf("signer: command %v, service %v, port %d", s.Command, s.Service, s.Port)
	}
	if s.Resources.Requests["memory"] != "2Gi" || s.Resources.Limits["memory"] != "2Gi" || s.Resources.Limits["cpu"] != "1" {
		t.Errorf("signer resources %+v: want memory request = limit = 2Gi, cpu limit 1", s.Resources)
	}
	for _, name := range []string{"crawler-post", "crawler-thread", "canary"} {
		if m := v.Components[name].Resources.Limits["memory"]; m != "1Gi" && m != "512Mi" {
			t.Errorf("%s: memory limit %s; without Chromium it needs at most 1Gi", name, m)
		}
	}
}
