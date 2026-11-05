package discovery

import (
	"context"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// Seeding the Iraq list loads every keyword once; seeding again adds
// nothing and never turns on a keyword someone turned off.
func TestSeedKeywords(t *testing.T) {
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	seed, err := IraqSeed()
	if err != nil {
		t.Fatal(err)
	}
	added, err := ImportSeed(ctx, db, seed)
	if err != nil || added != len(seed) {
		t.Fatalf("added %d of %d: %v", added, len(seed), err)
	}
	all, err := ListKeywords(ctx, db, KeywordFilter{Limit: 100000})
	if err != nil || len(all) != len(seed) {
		t.Fatalf("listed %d: %v", len(all), err)
	}
	var mosul Keyword
	off := 0
	for _, k := range all {
		if k.Term == "Mosul" {
			mosul = k
		}
		if !k.Enabled {
			off++
		}
	}
	if mosul.ID == 0 || !mosul.Enabled || off == 0 {
		t.Fatalf("Mosul %+v, %d off", mosul, off)
	}
	f := false
	if _, err := UpdateKeyword(ctx, db, mosul.ID, KeywordUpdate{Enabled: &f}); err != nil {
		t.Fatal(err)
	}
	if added, err := ImportSeed(ctx, db, seed); err != nil || added != 0 {
		t.Fatalf("second seed added %d: %v", added, err)
	}
	enabled := true
	on, _ := ListKeywords(ctx, db, KeywordFilter{Enabled: &enabled, Limit: 100000})
	for _, k := range on {
		if k.ID == mosul.ID {
			t.Fatal("seeding again turned a keyword back on")
		}
	}
	// A spelling that folds to a listed one is that one.
	k, created, err := AddKeyword(ctx, db, "MOSUL", "en", "", true)
	if err != nil || created || k.ID != mosul.ID {
		t.Errorf("MOSUL: %+v %v %v", k, created, err)
	}
	if _, _, err := AddKeyword(ctx, db, "Paris", "fr", "", true); err == nil {
		t.Error("an unknown language accepted")
	}
}

func TestParseKeywordCSV(t *testing.T) {
	kws, err := ParseKeywordCSV(strings.NewReader("term,language,group,enabled\nErbil,en,erbil,\nهەولێر,ckb,erbil,false\n"))
	if err != nil || len(kws) != 2 || !kws[0].Enabled || kws[1].Enabled || kws[1].Group != "erbil" {
		t.Fatalf("%+v %v", kws, err)
	}
	if _, err := ParseKeywordCSV(strings.NewReader("Erbil,en\nParis,fr\n")); err == nil {
		t.Error("an unknown language accepted after the first line")
	}
}
