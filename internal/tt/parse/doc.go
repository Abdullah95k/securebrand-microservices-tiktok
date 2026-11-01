// Package parse turns TikTok's answers into tt.v1 records: the profile page
// and the account call (Account), the post lists and the post page (Post,
// PostMetrics, Music, Media), comments and replies (Comment). The shapes are
// the ones the owner's recordings and the probe's runs saw
// (docs/tiktok/har-findings-2026-10-31.md, c2-findings, c3-findings).
//
// Parsers are shape-tolerant: fields are read from candidate paths, and a
// Report lists the required fields that could not be found (schema drift)
// and the optional ones (metrics only). Records the parser cannot key are
// dropped and counted in the result's Dropped, never keyed on something
// else (ADR 0007 §1): an id that does not fit int64, a person without a
// numeric user id. A comment author's predicted_age_group (TikTok's
// inference of their age band) is read into Person, by the owner's choice
// (ADR 0007 §14).
//
// The golden fixtures under testdata/tt are scrubbed real answers
// (internal/tt/scrub); every parser version must pass all of them.
package parse
