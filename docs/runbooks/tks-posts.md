# tks: posts that are no longer public

tks marks a stored post **unavailable** when TikTok stops showing it to a logged-out reader ([`discovery-plan.md` §17](../tiktok/discovery-plan.md#17-posts-that-are-no-longer-public-approved-in-m8), [ADR 0007 §17](../tiktok/adr/0007-tks-data-model-and-contracts.md#17-settled-in-m8b-posts-that-are-no-longer-public)). It is never "deleted": a logged-out reader cannot tell a deletion from a post made private or removed by TikTok. Nothing is deleted from the store (D8).

## How it works

1. **A candidate.** During any list walk (an incremental check, a metrics walk, a backfill), a full `creator` list page is compared with the account's stored visible posts dated between the page's oldest post and the cursor it was asked with. A stored post that no page of the walk returned is a candidate.
2. **A confirmation.** The crawler reads the post's page once, by plain HTTP, on the walk's identity:
   - the page shows the post: it is refreshed from the page and stays visible (`result="visible"`);
   - the page answers a code in `tt.posts.unavailable.codes` (10216, made private): the post is published as unavailable with the code and the exit's region (`unavailable`);
   - a code in `account_codes` (the account, not the post): nothing is marked (`account`);
   - any other code: nothing is marked, and the code is logged (`unknown_code`).
3. **Guards.** A page with fewer than `min_page_posts` posts marks nothing; more than `max_per_page` candidates on one page is a reading problem (`reading_problem`); a check reads at most `max_checks` post pages (`deferred`); a private, banned or gone account ends its check first, so its posts keep their status.
4. **Afterwards.** An unavailable post's page is not read again. If a list returns it later, it is visible again; `post_status_changes` keeps every change.

Read it in the API: `GET /v1/accounts/{account}/posts?status=unavailable` (each with `unavailable_since`, `status_code`, `status_region`). Metrics: `tks_post_vanish_total{result}`, `tks_post_status_changes_total{status}`.

To turn it off: `TKS_TT__POSTS__UNAVAILABLE__ENABLED=false` on the crawler.

## Unknown codes

`TksPostPagesUnknownCode`: post pages answered codes that tks does not know to mean a post is gone, so those posts stay visible.
1. Find the codes in the crawler's logs: `post page answered a statusCode not known to mean a post that is not public code=…`.
2. Do not add a code from a guess. Put it on the owner's next probe list (T34 in `open-questions.md`): the probe reads the page of a post known to be private, deleted or removed, and records the code.
3. Once confirmed, add it to `tt.posts.unavailable.codes` (or `account_codes` if it is about the account) in the deployment's config, and record it in T34. The next walks over those posts' dates mark them.

## Many posts missing at once

`TksPostsVanishingAtOnce`: full list pages left out more stored posts than `max_per_page`, so tks marked none of them.
1. Is the canary failing or promoting `creator_list` ([tks-canary.md](tks-canary.md))? Then the list changed: fix that first.
2. Look at one account in the crawler's logs (`many stored posts missing from one list page`): if one account removed many posts at once, the posts are marked over its later walks only if fewer go missing per page; raise `max_per_page` for a while, or leave them visible.
3. If many accounts show it, TikTok may be filtering the list for the exit's region (T30): compare with another tier.
