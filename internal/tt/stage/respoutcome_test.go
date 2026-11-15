package stage

import (
	"testing"

	tkstaskv1 "github.com/abdullah95k/securebrand-microservices-tiktok/gen/go/securebrand/tks/task/v1"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/graphql"
)

// A response the open breaker made up waits (no stall counted); a real
// drifted answer still counts against the task.
func TestRespOutcomeShortCircuitWaits(t *testing.T) {
	task := &tkstaskv1.Task{TaskId: "t", Kind: tkstaskv1.Kind_KIND_POST}
	c := map[string]int64{}
	o := respOutcome(task, &graphql.Response{Class: detect.SchemaDrift, Reason: "breaker open", ShortCircuit: true}, "", c)
	if o.Result != Retry || !o.Waiting || c["short_circuits"] != 1 {
		t.Errorf("short circuit: result %v, waiting %v, counters %v", o.Result, o.Waiting, c)
	}
	o = respOutcome(task, &graphql.Response{Class: detect.SchemaDrift, Reason: "[has_more]"}, "", map[string]int64{})
	if o.Result != Retry || o.Waiting {
		t.Errorf("drift: result %v, waiting %v", o.Result, o.Waiting)
	}
	if got := stallsAfter(2, false, true); got != 2 {
		t.Errorf("a wait counted a stall: %d", got)
	}
}
