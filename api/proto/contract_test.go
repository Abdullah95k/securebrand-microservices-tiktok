// Contract tests across services: tks's envelope and common.v1 against
// fbs's, kept here (beside the protos) because internal/tt may not import
// Facebook code (internal/tt/boundary_test.go).

package protosrc_test

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	commonv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/common/v1"
	fbv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fb/v1"
	fbstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/fbs/task/v1"
	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
)

// tks's envelope keeps fbs's generic fields (name, number and kind) so the
// copied worker runs on it; only the scope differs (ADR 0007 §2).
func TestEnvelopeKeepsFBSFields(t *testing.T) {
	fbs := (&fbstaskv1.Task{}).ProtoReflect().Descriptor()
	tks := (&tkstaskv1.Task{}).ProtoReflect().Descriptor()
	for i := 0; i < fbs.Fields().Len(); i++ {
		f := fbs.Fields().Get(i)
		if f.ContainingOneof() != nil {
			continue // the scope
		}
		g := tks.Fields().ByName(f.Name())
		if g == nil {
			t.Errorf("tks.task.v1.Task lacks %s", f.Name())
			continue
		}
		if g.Number() != f.Number() || g.Kind() != f.Kind() || g.Cardinality() != f.Cardinality() || g.IsMap() != f.IsMap() {
			t.Errorf("%s: tks %d %v, fbs %d %v", f.Name(), g.Number(), g.Kind(), f.Number(), f.Kind())
		}
	}
	for _, m := range [][2]protoreflect.MessageDescriptor{
		{(&fbstaskv1.Limits{}).ProtoReflect().Descriptor(), (&tkstaskv1.Limits{}).ProtoReflect().Descriptor()},
		{(&fbv1.Observation{}).ProtoReflect().Descriptor(), (&commonv1.Observation{}).ProtoReflect().Descriptor()},
	} {
		for i := 0; i < m[0].Fields().Len(); i++ {
			f := m[0].Fields().Get(i)
			g := m[1].Fields().ByName(f.Name())
			if g == nil || g.Number() != f.Number() || g.Kind() != f.Kind() {
				t.Errorf("%s.%s differs from fbs's", m[1].FullName(), f.Name())
			}
		}
	}
	// The crawl modes are fbs's values, so a mode survives the move.
	for v, name := range fbv1.CrawlMode_name {
		if commonv1.CrawlMode_name[v] != name {
			t.Errorf("crawl mode %d: %q, fbs %q", v, commonv1.CrawlMode_name[v], name)
		}
	}
}

// TikTok's kinds start at 200 and its scopes at field 40, disjoint from
// fbs's (1–4, 10–13) and igs's (100s, 20s).
func TestEnvelopeTikTokPartsDisjoint(t *testing.T) {
	for v := range tkstaskv1.Kind_name {
		if v != 0 && v < 200 {
			t.Errorf("kind %d below 200", v)
		}
	}
	scope := (&tkstaskv1.Task{}).ProtoReflect().Descriptor().Oneofs().ByName("scope")
	for i := 0; i < scope.Fields().Len(); i++ {
		if n := scope.Fields().Get(i).Number(); n < 40 {
			t.Errorf("scope field %s is %d, below 40", scope.Fields().Get(i).Name(), n)
		}
	}
	rr := (&tkstaskv1.Task{}).ProtoReflect().Descriptor().ReservedRanges()
	for _, n := range []protoreflect.FieldNumber{10, 13, 21, 29} {
		if !rr.Has(n) {
			t.Errorf("field %d is not reserved", n)
		}
	}
}

// A task on the wrong service's topic decodes with an unknown kind and no
// scope, so the worker dead-letters it instead of running it.
func TestForeignTasksDecodeAsUnknown(t *testing.T) {
	fb, err := proto.Marshal(&fbstaskv1.Task{TaskId: "t1", JobId: "j1", Kind: fbstaskv1.Kind_KIND_PAGE,
		Lane: fbstaskv1.Lane_LANE_P1, Scope: &fbstaskv1.Task_Page{Page: &fbstaskv1.PageScope{PageId: 42}}, Waits: 3})
	if err != nil {
		t.Fatal(err)
	}
	var asTKS tkstaskv1.Task
	if err := proto.Unmarshal(fb, &asTKS); err != nil {
		t.Fatal(err)
	}
	if _, known := tkstaskv1.Kind_name[int32(asTKS.Kind)]; known || asTKS.Scope != nil {
		t.Fatalf("an fbs task decoded as tks's: kind %v, scope %v", asTKS.Kind, asTKS.Scope)
	}
	if asTKS.TaskId != "t1" || asTKS.JobId != "j1" || asTKS.Waits != 3 {
		t.Fatalf("generic fields lost: %+v", &asTKS)
	}

	tk, err := proto.Marshal(&tkstaskv1.Task{TaskId: "t2", Kind: tkstaskv1.Kind_KIND_ACCOUNT,
		Scope: &tkstaskv1.Task_Account{Account: &tkstaskv1.AccountScope{UserId: 7}}})
	if err != nil {
		t.Fatal(err)
	}
	var asFBS fbstaskv1.Task
	if err := proto.Unmarshal(tk, &asFBS); err != nil {
		t.Fatal(err)
	}
	if _, known := fbstaskv1.Kind_name[int32(asFBS.Kind)]; known || asFBS.Scope != nil {
		t.Fatalf("a tks task decoded as fbs's: kind %v, scope %v", asFBS.Kind, asFBS.Scope)
	}
}
