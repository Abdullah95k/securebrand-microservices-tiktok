package topics

import (
	"context"
	"io/fs"
	"regexp"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"gopkg.in/yaml.v3"

	protosrc "github.com/abdullah95k/securebrand-microservices-tiktok/api/proto"
	"github.com/abdullah95k/securebrand-microservices-tiktok/configs"
	_ "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	_ "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tt/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/schemareg"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
)

func yamlTopics(t *testing.T) map[string]bool {
	t.Helper()
	b, err := configs.FS.ReadFile("tt/topics.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Topics []struct {
			Name string `yaml:"name"`
		} `yaml:"topics"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, tp := range doc.Topics {
		if out[tp.Name] {
			t.Fatalf("%s listed twice", tp.Name)
		}
		out[tp.Name] = true
	}
	return out
}

// Every topic tks uses is in configs/tt/topics.yaml and every topic there
// is one tks uses; all are tks's own names (ADR 0006 §5).
func TestTopicsMatchConfig(t *testing.T) {
	inYAML := yamlTopics(t)
	used := map[string]bool{TaskMedia: true, DLQ: true, PersistQuarantine: true}
	for tp := range ValueSchemas {
		used[tp] = true
	}
	for _, tp := range RetryTopics {
		used[tp] = true
	}
	for _, k := range []tkstaskv1.Kind{tkstaskv1.Kind_KIND_ACCOUNT, tkstaskv1.Kind_KIND_POST, tkstaskv1.Kind_KIND_THREAD, tkstaskv1.Kind_KIND_MEDIA} {
		for _, l := range []tkstaskv1.Lane{tkstaskv1.Lane_LANE_P0, tkstaskv1.Lane_LANE_P1, tkstaskv1.Lane_LANE_P2} {
			used[Task(k, l)] = true
		}
	}
	for _, tp := range Entity {
		if _, ok := ValueSchemas[tp]; !ok {
			t.Errorf("entity topic %s has no value schema", tp)
		}
	}
	own := regexp.MustCompile(`^(tt\.[a-z_.]+\.v1|tks\.[a-z0-9.]+)$`)
	for tp := range used {
		if !inYAML[tp] {
			t.Errorf("%s is used but not in configs/tt/topics.yaml", tp)
		}
	}
	for tp := range inYAML {
		if !used[tp] {
			t.Errorf("%s is in configs/tt/topics.yaml but no code names it", tp)
		}
		if !own.MatchString(tp) {
			t.Errorf("%s is not one of tks's names", tp)
		}
	}
}

// Each public topic's schema file is embedded for the registry and
// declares the message its tks-type header names.
func TestValueSchemas(t *testing.T) {
	if len(ValueSchemas) != len(ValueTypes) {
		t.Fatalf("%d schemas, %d types", len(ValueSchemas), len(ValueTypes))
	}
	for tp, file := range ValueSchemas {
		if _, err := fs.Stat(protosrc.FS, file); err != nil {
			t.Errorf("%s: %v", tp, err)
		}
		name := ValueTypes[tp]
		d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
		if err != nil {
			t.Errorf("%s: %s: %v", tp, name, err)
			continue
		}
		if _, ok := d.(protoreflect.MessageDescriptor); !ok || d.ParentFile().Path() != file {
			t.Errorf("%s: %s is declared in %s, not %s", tp, name, d.ParentFile().Path(), file)
		}
	}
}

func TestTaskTopic(t *testing.T) {
	if got := Task(tkstaskv1.Kind_KIND_ACCOUNT, tkstaskv1.Lane_LANE_P0); got != "tks.task.account.p0" {
		t.Fatal(got)
	}
	if got := Task(tkstaskv1.Kind_KIND_THREAD, tkstaskv1.Lane_LANE_UNSPECIFIED); got != "tks.task.thread.p1" {
		t.Fatal(got)
	}
	if got := Task(tkstaskv1.Kind_KIND_MEDIA, tkstaskv1.Lane_LANE_P2); got != TaskMedia {
		t.Fatal(got)
	}
	for _, l := range []string{"p0", "p1", "p2"} {
		if LaneName(ParseLane(l)) != l {
			t.Fatal(l)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("an unknown kind must panic")
		}
	}()
	Task(tkstaskv1.Kind(1), tkstaskv1.Lane_LANE_P1)
}

// `tksctl schemas apply` registers tks's contract with a real Schema
// Registry: every public topic's subject and the files they import,
// BACKWARD_TRANSITIVE, idempotently.
func TestSchemasApply(t *testing.T) {
	url := testenv.SchemaRegistry(t)
	c := &schemareg.Client{URL: url}
	ctx := context.Background()
	prefix := testenv.TopicPrefix()
	t.Cleanup(func() {
		subjects, _ := c.Subjects(context.Background())
		// Topic subjects reference the imports: delete them first.
		sort.Slice(subjects, func(i, j int) bool {
			return strings.HasSuffix(subjects[i], "-value") && !strings.HasSuffix(subjects[j], "-value")
		})
		for _, s := range subjects {
			if strings.HasPrefix(s, prefix) {
				_ = c.DeleteSubject(context.Background(), s)
			}
		}
	})
	if res, err := schemareg.Apply(ctx, c, protosrc.FS, prefix, ValueSchemas, true); err != nil {
		t.Fatalf("check: %v %+v", err, res)
	}
	res, err := schemareg.Apply(ctx, c, protosrc.FS, prefix, ValueSchemas, false)
	if err != nil {
		t.Fatal(err)
	}
	subjects := map[string]int{}
	for _, r := range res {
		if r.Version != 1 || !r.Compatible {
			t.Fatalf("%+v", r)
		}
		subjects[r.Subject] = r.ID
	}
	// Every topic, plus the two common.proto files the schemas import.
	for _, imp := range []string{"securebrand/common/v1/common.proto", "securebrand/tt/v1/common.proto"} {
		if _, ok := subjects[prefix+imp]; !ok {
			t.Errorf("import %s not registered", imp)
		}
	}
	if len(subjects) != len(ValueSchemas)+2 {
		t.Fatalf("subjects: %v", subjects)
	}
	if lvl, err := c.Compatibility(ctx, prefix+Comment+"-value"); err != nil || lvl != schemareg.BackwardTransitive {
		t.Fatalf("compatibility %q %v", lvl, err)
	}
	again, err := schemareg.Apply(ctx, c, protosrc.FS, prefix, ValueSchemas, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range again {
		if r.ID != subjects[r.Subject] || r.Version != 1 {
			t.Fatalf("not idempotent: %+v", r)
		}
	}
}
