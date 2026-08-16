# Code-review notes: reviewer model and provenance

## Scope

This change adds confidential reviewer profiles, per-KG familiarity, pseudonymous
review attribution, formulation authorship/approval links, legacy sanitized
export migration, and review-workbench propagation. It intentionally does not
add the reviewer profile form; that is the next change.

## Security invariants to check

- Files under `confidential/reviewers/*.json*` are Git-ignored.
- Only `reviewer-NNNN` identifiers cross the confidential boundary.
- No profile or KG-familiarity field is copied into bundles, exports,
  benchmarks, logs, or tests.
- Migration accepts only explicit sanitized paths below
  `var/review/exports/` and rejects private/mixed exports.
- Existing `benchmark/v1` through `benchmark/v10` files are untouched.
- Holdout paths were neither read nor migrated.

## Provenance semantics

- `prior_review_ids` describes the review chain.
- The export map key remains a bundle lookup key; the nested `review_id` is the
  reviewer-specific event identity used by provenance links.
- `authored_formulation_ids` identifies wording or SPARQL written by the
  reviewer.
- `approved_formulation_ids` identifies formulations accepted by the review;
  approval does not imply authorship.
- Reviewer backlinks are derived by querying these fields, not stored on the
  confidential profile.
- New bundles require an explicit pseudonymous reviewer ID. Legacy sanitized
  exports have an explicit migration path.
- Browser storage keys include the reviewer ID; only `reviewer-0001` can import
  the pre-multi-reviewer legacy keys.

## Suggested review focus

Check comparison-mode reuse carefully: copying a prior decision links the prior
review and must not attribute copied wording to the current reviewer. Check
that public benchmark provenance includes pseudonymous IDs and formulation links
only, while internal review records retain the normalized link arrays.
