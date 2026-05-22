#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SCRIPT_VERSION = "evaluate_runs.py@v1"

JUDGE_PROMPT = """You are evaluating a generated natural-language question for a fixed SPARQL query.

The SPARQL query is fixed input, not generated output. Do not grade SPARQL equivalence.

Score whether the candidate question faithfully captures the SPARQL intent and is equivalent to the gold question.
Return JSON only with exactly these fields:
- sparql_faithfulness: "pass", "partial", or "fail"
- gold_equivalence: "pass", "partial", or "fail"
- semantic_score: integer 1-5
- issues: array of strings from this set when applicable:
  missing_constraint, extra_constraint, wrong_answer_type, too_specific, too_vague,
  ontology_jargon, awkward_wording, placeholder_leak, unsupported_by_sparql
- rationale: 1-2 short sentences

Scoring:
5 = faithful and equivalent, no material issue.
4 = faithful with minor wording issue.
3 = mostly faithful but partial, ambiguous, or missing a secondary constraint.
2 = major mismatch but some overlap.
1 = wrong or unusable.
"""


def stable_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_text(stable_json_dumps(value))


def load_json_records(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8", errors="ignore")
    stripped = raw.lstrip("\ufeff").lstrip()
    if not stripped:
        return []
    if stripped.startswith("["):
        data = json.loads(stripped)
        return [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []

    rows: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip().lstrip("\ufeff")
        if not line:
            continue
        item = json.loads(line)
        if isinstance(item, dict):
            rows.append(item)
    return rows


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, records: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def extract_first_json_object(text: str) -> Optional[Dict[str, Any]]:
    text = text.strip()
    if not text:
        return None
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escaped = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start : idx + 1]
                try:
                    data = json.loads(candidate)
                    return data if isinstance(data, dict) else None
                except json.JSONDecodeError:
                    return None
    return None


def canonical_sparql(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    lines = [re.sub(r"[ \t]+$", "", line) for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    return "\n".join(lines).strip()


def normalize_question(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    return re.sub(r"\s+", " ", text).strip()


def output_payload(output_record: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(output_record, dict):
        return {}
    payload = output_record.get("llm_output")
    return payload if isinstance(payload, dict) else {}


def validate_output_shape(payload: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    required = [
        "ranked_evidence_phrases",
        "nl_question",
        "nl_question_origin",
        "confidence",
        "confidence_rationale",
        "needs_review",
    ]
    for key in required:
        if key not in payload:
            errors.append(f"missing_{key}")
    if errors:
        return errors

    if not isinstance(payload.get("ranked_evidence_phrases"), list):
        errors.append("ranked_evidence_phrases_not_array")
    if not normalize_question(payload.get("nl_question")):
        errors.append("empty_nl_question")
    origin = payload.get("nl_question_origin")
    if not isinstance(origin, dict):
        errors.append("nl_question_origin_not_object")
    else:
        if origin.get("mode") not in {"verbatim", "paraphrased", "generated"}:
            errors.append("bad_origin_mode")
        if not isinstance(origin.get("evidence_ids"), list):
            errors.append("origin_evidence_ids_not_array")
        primary = origin.get("primary_evidence_id")
        if primary is not None and not isinstance(primary, str):
            errors.append("bad_primary_evidence_id")
    confidence = payload.get("confidence")
    if not isinstance(confidence, int) or not 0 <= confidence <= 100:
        errors.append("bad_confidence")
    if not isinstance(payload.get("confidence_rationale"), str) or not payload.get("confidence_rationale", "").strip():
        errors.append("empty_confidence_rationale")
    if not isinstance(payload.get("needs_review"), bool):
        errors.append("needs_review_not_bool")
    return errors


def collect_input_evidence_ids(input_record: Optional[Dict[str, Any]]) -> set[str]:
    if not isinstance(input_record, dict):
        return set()
    evidence = input_record.get("evidence")
    if not isinstance(evidence, list):
        return set()
    return {str(ev.get("evidence_id")) for ev in evidence if isinstance(ev, dict) and ev.get("evidence_id")}


def evidence_id_warnings(payload: Dict[str, Any], valid_ids: set[str]) -> List[str]:
    warnings: List[str] = []
    used: List[str] = []
    origin = payload.get("nl_question_origin")
    if isinstance(origin, dict):
        evidence_ids = origin.get("evidence_ids")
        if isinstance(evidence_ids, list):
            used.extend(str(eid) for eid in evidence_ids)
        primary = origin.get("primary_evidence_id")
        if primary:
            used.append(str(primary))
    phrases = payload.get("ranked_evidence_phrases")
    if isinstance(phrases, list):
        for phrase in phrases:
            if isinstance(phrase, dict) and phrase.get("evidence_id"):
                used.append(str(phrase["evidence_id"]))
    missing = sorted({eid for eid in used if eid not in valid_ids})
    if missing:
        warnings.append("unknown_evidence_ids:" + ",".join(missing))
    return warnings


def placeholder_warnings(question: str) -> List[str]:
    if re.search(r"%[a-zA-Z]", question):
        return ["placeholder_leak"]
    if re.search(r"\{[^{}]+\}", question):
        return ["brace_placeholder_leak"]
    return []


def load_benchmark(benchmark_dir: Path) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    manifest = load_json(benchmark_dir / "manifest.json")
    new_layout = (benchmark_dir / "approved.jsonl").exists() or "approved" in manifest.get("files", {})
    benchmark = load_json_records(benchmark_dir / "benchmark.jsonl")
    approved = load_json_records(benchmark_dir / "approved.jsonl") if new_layout else benchmark
    pending = load_json_records(benchmark_dir / "pending.jsonl")
    dismissed = load_json_records(benchmark_dir / "dismissed.jsonl")
    items: List[Dict[str, Any]] = []
    if new_layout:
        for rec in benchmark:
            item = dict(rec)
            item["_benchmark_status_group"] = str(rec.get("benchmark_status_group") or rec.get("_benchmark_status_group") or "")
            items.append(item)
    else:
        for rec in approved:
            item = dict(rec)
            item["_benchmark_status_group"] = "approved"
            items.append(item)
        for rec in pending:
            item = dict(rec)
            item["_benchmark_status_group"] = "pending"
            items.append(item)
    counts = {
        "benchmark": len(items),
        "approved": len(approved),
        "pending": len(pending),
        "dismissed": len(dismissed),
        "holdout": int(manifest.get("counts", {}).get("holdout", 0)) if isinstance(manifest.get("counts"), dict) else 0,
    }
    return items, dismissed, counts


def load_run(run_path: Path) -> Dict[str, Any]:
    if not run_path.exists():
        raise FileNotFoundError(f"Missing run path: {run_path}")
    run_id = run_path.name if run_path.is_dir() else run_path.stem
    if run_path.is_dir():
        output_path = run_path / "llm_outputs.jsonl"
        input_path = run_path / "llm_inputs.jsonl"
        manifest_path = run_path / "manifest.json"
    else:
        output_path = run_path
        input_path = run_path.with_name("llm_inputs.jsonl")
        manifest_path = run_path.with_name("manifest.json")

    outputs = load_json_records(output_path)
    inputs = load_json_records(input_path)
    manifest = load_json(manifest_path)
    generation_run_id = str(manifest.get("generation_run_id") or manifest.get("run_id") or run_id)
    return {
        "run_id": run_id,
        "generation_run_id": generation_run_id,
        "path": str(run_path),
        "output_path": str(output_path),
        "input_path": str(input_path),
        "manifest_path": str(manifest_path),
        "manifest": manifest,
        "outputs": {str(rec.get("query_id")): rec for rec in outputs if rec.get("query_id")},
        "inputs": {str(rec.get("query_id")): rec for rec in inputs if rec.get("query_id")},
        "output_count": len(outputs),
        "input_count": len(inputs),
    }


def load_judge_cache(path: Path) -> Dict[str, Dict[str, Any]]:
    cache: Dict[str, Dict[str, Any]] = {}
    for rec in load_json_records(path):
        key = rec.get("cache_key")
        result = rec.get("result")
        if isinstance(key, str) and isinstance(result, dict):
            cache[key] = result
    return cache


def validate_judge_result(result: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {"pass", "partial", "fail"}
    out = {
        "sparql_faithfulness": result.get("sparql_faithfulness"),
        "gold_equivalence": result.get("gold_equivalence"),
        "semantic_score": result.get("semantic_score"),
        "issues": result.get("issues"),
        "rationale": result.get("rationale"),
    }
    if out["sparql_faithfulness"] not in allowed:
        out["sparql_faithfulness"] = "fail"
    if out["gold_equivalence"] not in allowed:
        out["gold_equivalence"] = "fail"
    if not isinstance(out["semantic_score"], int) or not 1 <= out["semantic_score"] <= 5:
        out["semantic_score"] = 1
    if not isinstance(out["issues"], list):
        out["issues"] = []
    out["issues"] = [str(item) for item in out["issues"]]
    if not isinstance(out["rationale"], str) or not out["rationale"].strip():
        out["rationale"] = "Judge returned no rationale."
    return out


def run_judge(
    *,
    client: Any,
    model: str,
    timeout_s: float,
    sparql: str,
    gold_question: str,
    candidate_question: str,
) -> Dict[str, Any]:
    del timeout_s
    user_payload = {
        "sparql": sparql,
        "gold_question": gold_question,
        "candidate_question": candidate_question,
    }
    resp = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": JUDGE_PROMPT},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, indent=2)},
        ],
    )
    parsed = extract_first_json_object((resp.output_text or "").strip())
    if parsed is None:
        raise ValueError("No JSON object found in judge output")
    return validate_judge_result(parsed)


def score_item(
    *,
    benchmark_item: Dict[str, Any],
    run: Dict[str, Any],
    baseline_run: Optional[Dict[str, Any]],
    judge_cache: Dict[str, Dict[str, Any]],
    judge_cache_records: List[Dict[str, Any]],
    judge_client: Any,
    judge_model: str,
    judge_timeout_s: float,
    skip_judge: bool,
) -> Dict[str, Any]:
    query_id = str(benchmark_item.get("query_id") or "")
    run_id = run["run_id"]
    output_record = run["outputs"].get(query_id)
    input_record = run["inputs"].get(query_id)
    payload = output_payload(output_record)
    question = normalize_question(payload.get("nl_question"))
    gold_question = normalize_question(benchmark_item.get("gold_question"))

    warnings: List[str] = []
    errors: List[str] = []
    if output_record is None:
        errors.append("missing_output")
    if input_record is None:
        warnings.append("missing_input")

    errors.extend(validate_output_shape(payload) if output_record is not None else [])
    warnings.extend(placeholder_warnings(question))
    warnings.extend(evidence_id_warnings(payload, collect_input_evidence_ids(input_record)))

    benchmark_sparql = canonical_sparql(benchmark_item.get("sparql"))
    input_sparql = canonical_sparql(input_record.get("sparql_clean") if isinstance(input_record, dict) else "")
    sparql_match = bool(input_sparql) and benchmark_sparql == input_sparql
    if input_record is not None and not sparql_match:
        warnings.append("sparql_mismatch")

    baseline_question = ""
    question_changed_from_baseline: Optional[bool] = None
    if baseline_run is not None:
        baseline_payload = output_payload(baseline_run["outputs"].get(query_id))
        baseline_question = normalize_question(baseline_payload.get("nl_question"))
        question_changed_from_baseline = question != baseline_question if baseline_question and question else None

    judge: Optional[Dict[str, Any]] = None
    judge_status = "not_run"
    deterministic_blockers = set(errors)
    incompatible = "missing_input" in warnings or "sparql_mismatch" in warnings
    if deterministic_blockers:
        judge_status = "skipped_output_invalid"
    elif incompatible:
        judge_status = "skipped_input_incompatible"
    elif skip_judge:
        judge_status = "skipped_by_flag"
    else:
        cache_key = sha256_json(
            {
                "judge_model": judge_model,
                "judge_prompt_hash": sha256_text(JUDGE_PROMPT),
                "sparql": benchmark_sparql,
                "gold_question": gold_question,
                "candidate_question": question,
            }
        )
        if cache_key in judge_cache:
            judge = judge_cache[cache_key]
            judge_status = "cached"
        else:
            try:
                started = time.time()
                judge = run_judge(
                    client=judge_client,
                    model=judge_model,
                    timeout_s=judge_timeout_s,
                    sparql=benchmark_sparql,
                    gold_question=gold_question,
                    candidate_question=question,
                )
                judge["elapsed_ms"] = int((time.time() - started) * 1000)
                judge_cache[cache_key] = judge
                judge_cache_records.append(
                    {
                        "cache_key": cache_key,
                        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                        "judge_model": judge_model,
                        "judge_prompt_hash": sha256_text(JUDGE_PROMPT),
                        "result": judge,
                    }
                )
                judge_status = "ran"
            except Exception as exc:
                warnings.append(f"judge_error:{exc}")
                judge_status = "error"

    return {
        "run_id": run_id,
        "generation_run_id": run_id,
        "benchmark_id": benchmark_item.get("benchmark_id"),
        "benchmark_status_group": benchmark_item.get("_benchmark_status_group"),
        "review_status": benchmark_item.get("review_status"),
        "kg_id": benchmark_item.get("kg_id"),
        "query_id": query_id,
        "query_label": benchmark_item.get("query_label"),
        "sparql_match": sparql_match,
        "candidate_question": question,
        "gold_question": gold_question,
        "baseline_question": baseline_question,
        "question_changed_from_baseline": question_changed_from_baseline,
        "deterministic": {
            "errors": errors,
            "warnings": warnings,
        },
        "judge_status": judge_status,
        "judge": judge,
        "model": output_record.get("model") if isinstance(output_record, dict) else None,
        "request_config": output_record.get("request_config") if isinstance(output_record, dict) else None,
        "run_signature": output_record.get("run_signature") if isinstance(output_record, dict) else None,
    }


def summarize(scores: List[Dict[str, Any]], dismissed_count: int, holdout_count: int, baseline_run_id: Optional[str]) -> Dict[str, Any]:
    by_run: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for score in scores:
        by_run[str(score.get("run_id"))].append(score)

    summary: Dict[str, Any] = {
        "dismissed_excluded": dismissed_count,
        "holdout_excluded": holdout_count,
        "baseline_run_id": baseline_run_id,
        "baseline_generation_run_id": baseline_run_id,
        "runs": {},
    }
    baseline_scores = {
        str(score.get("query_id")): score
        for score in scores
        if baseline_run_id and score.get("run_id") == baseline_run_id
    }
    for run_id, rows in sorted(by_run.items()):
        semantic_scores = [
            row["judge"]["semantic_score"]
            for row in rows
            if isinstance(row.get("judge"), dict) and isinstance(row["judge"].get("semantic_score"), int)
        ]
        deterministic_errors = Counter(err for row in rows for err in row["deterministic"]["errors"])
        deterministic_warnings = Counter(warn.split(":", 1)[0] for row in rows for warn in row["deterministic"]["warnings"])
        regressions = 0
        improvements = 0
        if baseline_scores and run_id != baseline_run_id:
            for row in rows:
                base = baseline_scores.get(str(row.get("query_id")))
                if not base or not isinstance(base.get("judge"), dict) or not isinstance(row.get("judge"), dict):
                    continue
                base_score = base["judge"].get("semantic_score")
                row_score = row["judge"].get("semantic_score")
                if isinstance(base_score, int) and isinstance(row_score, int):
                    if base_score - row_score >= 2:
                        regressions += 1
                    if row_score - base_score >= 2:
                        improvements += 1
        summary["runs"][run_id] = {
            "items": len(rows),
            "coverage": sum(1 for row in rows if "missing_output" not in row["deterministic"]["errors"]),
            "sparql_mismatches": sum(1 for row in rows if "sparql_mismatch" in row["deterministic"]["warnings"]),
            "missing_inputs": sum(1 for row in rows if "missing_input" in row["deterministic"]["warnings"]),
            "deterministic_errors": dict(deterministic_errors),
            "deterministic_warnings": dict(deterministic_warnings),
            "judge_ran_or_cached": sum(1 for row in rows if row.get("judge_status") in {"ran", "cached"}),
            "mean_semantic_score": round(sum(semantic_scores) / len(semantic_scores), 3) if semantic_scores else None,
            "semantic_passes": sum(
                1
                for row in rows
                if isinstance(row.get("judge"), dict)
                and row["judge"].get("sparql_faithfulness") == "pass"
                and row["judge"].get("gold_equivalence") == "pass"
            ),
            "regressions_vs_baseline": regressions,
            "improvements_vs_baseline": improvements,
        }
    return summary


def render_summary_md(summary: Dict[str, Any], scores: List[Dict[str, Any]]) -> str:
    lines = [
        "# Evaluation Summary",
        "",
        f"- Baseline: `{summary.get('baseline_run_id') or ''}`",
        f"- Dismissed benchmark items excluded from scoring: {summary.get('dismissed_excluded', 0)}",
        f"- Private holdout items excluded from scoring: {summary.get('holdout_excluded', 0)}",
        "",
        "## Runs",
        "",
        "| Run | Items | Coverage | SPARQL warnings | Judge scored | Mean semantic | Regressions | Improvements |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for run_id, row in summary["runs"].items():
        mean = "" if row["mean_semantic_score"] is None else str(row["mean_semantic_score"])
        lines.append(
            f"| `{run_id}` | {row['items']} | {row['coverage']} | {row['sparql_mismatches']} | "
            f"{row['judge_ran_or_cached']} | {mean} | {row['regressions_vs_baseline']} | {row['improvements_vs_baseline']} |"
        )
    problem_rows = [
        row
        for row in scores
        if row["deterministic"]["errors"] or row["deterministic"]["warnings"] or row.get("judge_status") == "error"
    ][:30]
    if problem_rows:
        lines.extend(["", "## First Warnings And Errors", ""])
        for row in problem_rows:
            issues = row["deterministic"]["errors"] + row["deterministic"]["warnings"]
            lines.append(
                f"- `{row['run_id']}` `{row['query_label']}`: {', '.join(issues) or row.get('judge_status')}"
            )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate LLM generation runs against a reviewed benchmark.")
    parser.add_argument("--benchmark", default="benchmark/v2")
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument("--baseline", default="")
    parser.add_argument("--judge-model", default="gpt-5")
    parser.add_argument("--judge-timeout-s", type=float, default=120.0)
    parser.add_argument("--skip-judge", action="store_true", help="Run deterministic checks only.")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    benchmark_dir = Path(args.benchmark)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    benchmark_items, dismissed_items, benchmark_counts = load_benchmark(benchmark_dir)
    runs = [load_run(Path(path)) for path in args.runs]
    baseline_run: Optional[Dict[str, Any]] = None
    if args.baseline:
        baseline_target = str(Path(args.baseline))
        for run in runs:
            if run["path"] == baseline_target or run["run_id"] == Path(args.baseline).name:
                baseline_run = run
                break
        if baseline_run is None:
            baseline_run = load_run(Path(args.baseline))

    judge_cache_path = out_dir / "judge_cache.jsonl"
    judge_cache = load_judge_cache(judge_cache_path)
    judge_cache_records: List[Dict[str, Any]] = []
    judge_client = None
    if not args.skip_judge:
        from openai import OpenAI

        judge_client = OpenAI(timeout=args.judge_timeout_s)

    scores: List[Dict[str, Any]] = []
    for run in runs:
        for item in benchmark_items:
            scores.append(
                score_item(
                    benchmark_item=item,
                    run=run,
                    baseline_run=baseline_run,
                    judge_cache=judge_cache,
                    judge_cache_records=judge_cache_records,
                    judge_client=judge_client,
                    judge_model=args.judge_model,
                    judge_timeout_s=args.judge_timeout_s,
                    skip_judge=args.skip_judge,
                )
            )

    summary = summarize(
        scores,
        dismissed_count=len(dismissed_items),
        holdout_count=benchmark_counts.get("holdout", 0),
        baseline_run_id=baseline_run["run_id"] if baseline_run else None,
    )
    manifest = {
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "script_version": SCRIPT_VERSION,
        "benchmark": str(benchmark_dir),
        "benchmark_counts": benchmark_counts,
        "scored_status_groups": ["approved", "pending"],
        "dismissed_excluded": len(dismissed_items),
        "holdout_excluded": benchmark_counts.get("holdout", 0),
        "runs": [
            {
                "run_id": run["run_id"],
                "generation_run_id": run["generation_run_id"],
                "path": run["path"],
                "output_count": run["output_count"],
                "input_count": run["input_count"],
                "manifest": run["manifest"],
            }
            for run in runs
        ],
        "baseline_run_id": baseline_run["run_id"] if baseline_run else None,
        "baseline_generation_run_id": baseline_run["generation_run_id"] if baseline_run else None,
        "judge": {
            "enabled": not args.skip_judge,
            "model": args.judge_model,
            "prompt_hash": sha256_text(JUDGE_PROMPT),
            "timeout_s": args.judge_timeout_s,
        },
        "summary": summary,
    }

    write_json(out_dir / "manifest.json", manifest)
    write_jsonl(out_dir / "scores.jsonl", scores)
    if judge_cache_records:
        existing = load_json_records(judge_cache_path)
        write_jsonl(judge_cache_path, existing + judge_cache_records)
    elif not judge_cache_path.exists():
        write_jsonl(judge_cache_path, [])
    (out_dir / "summary.md").write_text(render_summary_md(summary, scores), encoding="utf-8")

    print(f"Wrote {len(scores)} score records to {out_dir / 'scores.jsonl'}")
    print(f"Wrote summary to {out_dir / 'summary.md'}")


if __name__ == "__main__":
    main()
