from __future__ import annotations

from scripts.check_public_tree import disallowed_reviewer_ids

# A negative fixture cannot be written as a literal: this file is tracked, and
# the rule under test would refuse it — which is the rule working. The IDs are
# assembled at runtime instead, and they are invented rather than real.
PREFIX = "reviewer-"


def rid(digits: str) -> str:
    return PREFIX + digits


def test_a_pseudonym_outside_the_reserved_block_is_refused():
    """The approved notice forbids publishing stable external reviewer IDs.

    This repository is public, so `git add` is the boundary that matters, not
    the release build.
    """
    assert disallowed_reviewer_ids(f"attributed to {rid('1111')} on this pair") == [rid("1111")]


def test_an_id_embedded_in_a_formulation_identifier_is_refused():
    text = f"europeana::europeana-0019::run::{rid('2222')}::formulation::candidate"
    assert disallowed_reviewer_ids(text) == [rid("2222")]


def test_the_reserved_synthetic_block_and_the_owner_example_are_allowed():
    assert disallowed_reviewer_ids("reviewer-9042 and reviewer-9999") == []
    assert disallowed_reviewer_ids("reviewer-0001 is the documented example") == []


def test_several_offenders_are_reported_once_each_and_sorted():
    text = f"{rid('2222')} {rid('1111')} {rid('2222')} reviewer-9042"
    assert disallowed_reviewer_ids(text) == [rid("1111"), rid("2222")]


def test_a_longer_pseudonym_outside_the_block_is_still_refused():
    assert disallowed_reviewer_ids(rid("90421")) == [rid("90421")]


def test_unrelated_text_is_untouched():
    assert disallowed_reviewer_ids("reviewer ID, reviewers, review-0042") == []
