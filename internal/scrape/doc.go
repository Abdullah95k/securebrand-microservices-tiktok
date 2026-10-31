// Package scrape is the root of the platform-neutral client layer that igs
// took over from fbs (ADR 0004 §2) and tks took over from igs (docs/tiktok
// ADR 0006): transport, response classes, proxies, rate limits, the query
// registry, doc_id discovery, the HTML pattern engine, the JSON walker, the
// identity pool and page loader, the GraphQL client and the browser engine.
//
// Nothing under internal/scrape may import platform code (internal/fb,
// internal/tt, their fakes and generated protos); boundary_test.go enforces
// it. A platform plugs in through data (rules, patterns, seeds) and small
// hooks instead: session.Hooks for its pages, a graphql.RequestBuilder for
// its requests (Facebook's are in internal/fb/client).
package scrape
