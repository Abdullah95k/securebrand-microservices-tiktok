// Package parse turns Facebook GraphQL/HTML payloads into securebrand.fb.v1
// messages. Parsers locate entities by __typename instead of fixed paths and
// try several candidate paths per field, so they survive the frequent A/B
// shape changes Facebook ships. Missing required fields are reported so the
// caller can classify the response as schema drift. The walker and the
// neutral field helpers are in internal/scrape/walk.
package parse
