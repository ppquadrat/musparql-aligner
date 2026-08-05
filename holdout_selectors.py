"""Validation for annotation-free, identity-visible holdout selectors."""
from __future__ import annotations

import re
from argparse import ArgumentParser, ArgumentTypeError, Namespace
from typing import Any, Mapping, Tuple


ALLOWED_SELECTOR_FIELDS = {"kg_id", "query_id", "sparql_version", "sparql_hash"}
SHA256_RE = re.compile(r"[0-9a-f]{64}")


def nonempty_selector_path(value: str) -> str:
    if not value.strip():
        raise ArgumentTypeError("holdout selector path must not be empty")
    return value


def add_holdout_filter_arguments(parser: ArgumentParser) -> None:
    """Require an explicit holdout-handling choice on agent-facing builders."""
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--holdout-selectors",
        default=None,
        metavar="PATH",
        type=nonempty_selector_path,
        help="Annotation-free selector JSON/JSONL for the identity-visible holdout policy.",
    )
    group.add_argument(
        "--no-holdout",
        action="store_true",
        help="Human assertion that no holdout identities currently exist.",
    )
    group.add_argument(
        "--holdout-filtered-upstream",
        action="store_true",
        help="Human assertion that an identity-private process already removed all holdout pairs from these inputs.",
    )


def holdout_input_policy(args: Namespace) -> str:
    if getattr(args, "holdout_selectors", ""):
        return "identity_visible_selectors"
    if getattr(args, "holdout_filtered_upstream", False):
        return "identity_private_filtered_upstream"
    return "no_holdout"


def validate_selector_record(record: Mapping[str, Any]) -> Tuple[str, str]:
    if set(record) - ALLOWED_SELECTOR_FIELDS:
        raise ValueError("Holdout selector files may contain identity/version fields only")
    kg_id = record.get("kg_id")
    query_id = record.get("query_id")
    if not isinstance(kg_id, str) or not kg_id.strip():
        raise ValueError("Each holdout selector requires a nonempty string kg_id")
    if not isinstance(query_id, str) or not query_id.strip():
        raise ValueError("Each holdout selector requires a nonempty string query_id")

    has_version = "sparql_version" in record
    has_hash = "sparql_hash" in record
    if has_version != has_hash:
        raise ValueError("Holdout selector SPARQL version and hash must be supplied together")
    if has_version:
        version = record.get("sparql_version")
        digest = record.get("sparql_hash")
        if isinstance(version, bool) or not isinstance(version, int) or version < 0:
            raise ValueError("Holdout selector sparql_version must be a non-negative integer")
        if not isinstance(digest, str) or SHA256_RE.fullmatch(digest) is None:
            raise ValueError("Holdout selector sparql_hash must be a lowercase SHA-256 digest")
    return kg_id, query_id


def assert_selectors_unedited(
    selector_keys: set[Tuple[str, str]], records: list[Mapping[str, Any]]
) -> None:
    """Reject selectors for query identities that retain any SPARQL edit."""
    from sparql_corrections import retained_sparql_edit_count

    by_key = {
        (str(record.get("kg_id") or ""), str(record.get("query_id") or "")): record
        for record in records
    }
    missing = sorted(selector_keys - set(by_key))
    if missing:
        raise ValueError(f"Holdout selectors do not resolve to canonical query records: {missing}")
    edited = sorted(
        key for key in selector_keys if retained_sparql_edit_count(by_key[key]) > 0
    )
    if edited:
        raise ValueError(
            "SPARQL-edited query identities are permanently ineligible for holdout: "
            + ", ".join(f"{kg_id}/{query_id}" for kg_id, query_id in edited)
        )
