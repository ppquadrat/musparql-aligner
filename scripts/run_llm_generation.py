#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import unicodedata

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - exercised only in minimal test envs
    OpenAI = None  # type: ignore[assignment]

SCRIPT_VERSION = "run_llm_generation.py@v4"
REPAIR_MIN_SCORE = 0.80
REPAIR_MIN_MARGIN = 0.35
REPAIR_TIE_MARGIN = 0.05


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def stable_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_text(stable_json_dumps(value))


def load_json_records(path: Path) -> tuple[List[Dict[str, Any]], bool]:
    if not path.exists():
        return [], False
    raw = path.read_text(encoding="utf-8", errors="ignore")
    stripped = raw.lstrip("\ufeff").lstrip()
    if not stripped:
        return [], False
    if stripped.startswith("["):
        try:
            data = json.loads(stripped)
        except json.JSONDecodeError:
            return [], True
        if not isinstance(data, list):
            return [], True
        return [item for item in data if isinstance(item, dict)], True

    rows: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip().lstrip("\ufeff")
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict):
            rows.append(rec)
    return rows, False


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    return load_json_records(path)[0]


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def log(message: str) -> None:
    try:
        print(message, flush=True)
    except BrokenPipeError:
        pass


def ensure_jsonl_file(path: Path) -> List[Dict[str, Any]]:
    records, was_array = load_json_records(path)
    if was_array:
        write_jsonl(path, records)
    return records


def build_completion_key(
    query_id: Any,
    query_label: Any,
    kg_id: Any,
    model: Any,
    system_prompt_hash: Any,
    input_hash: Any,
    request_config_hash: Any = "",
) -> tuple[str, str, str, str, str, str]:
    return (
        str(query_id),
        str(query_label),
        str(kg_id),
        str(model),
        str(system_prompt_hash),
        f"{input_hash}:{request_config_hash}" if request_config_hash else str(input_hash),
    )


def extract_completion_key(rec: Dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    signature = rec.get("run_signature")
    if isinstance(signature, dict):
        return build_completion_key(
            rec.get("query_id"),
            rec.get("query_label"),
            rec.get("kg_id"),
            signature.get("model"),
            signature.get("system_prompt_hash"),
            signature.get("input_hash"),
            signature.get("request_config_hash", ""),
        )
    return build_completion_key(
        rec.get("query_id"),
        rec.get("query_label"),
        rec.get("kg_id"),
        rec.get("model"),
        "",
        "",
    )


def load_completed(path: Path) -> set[tuple[str, str, str, str, str, str]]:
    completed: set[tuple[str, str, str, str, str, str]] = set()
    for rec in ensure_jsonl_file(path):
        if not isinstance(rec, dict):
            continue
        completed.add(extract_completion_key(rec))
    return completed


def load_examples(path: Optional[Path], limit: int = 2) -> str:
    if path is None or not path.exists():
        return ""
    examples = load_jsonl(path)[:limit]
    if not examples:
        return ""
    return json.dumps(examples, ensure_ascii=False, indent=2)


def extract_first_json_object(text: str) -> Optional[Dict[str, Any]]:
    text = text.strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    # Fallback: brace scan for first valid object.
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start : i + 1]
                try:
                    parsed = json.loads(candidate)
                    if isinstance(parsed, dict):
                        return parsed
                except json.JSONDecodeError:
                    return None
    return None


def validate_output(obj: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    try:
        import jsonschema  # type: ignore
    except Exception:
        required = schema.get("required", [])
        missing = [k for k in required if k not in obj]
        if missing:
            return False, f"Missing required keys: {missing}"
        return True, None

    try:
        jsonschema.validate(instance=obj, schema=schema)
        return True, None
    except Exception as e:
        return False, str(e)


def normalize_citation_text(text: Any) -> str:
    value = unicodedata.normalize("NFKC", str(text or ""))
    value = value.replace("\u2018", "'").replace("\u2019", "'")
    value = value.replace("\u201c", '"').replace("\u201d", '"')
    value = re.sub(r"[\u2010-\u2015]", "-", value)
    value = re.sub(r"(\w)-\s+(\w)", r"\1\2", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip().casefold()


def citation_tokens(text: Any) -> List[str]:
    normalized = normalize_citation_text(text)
    return [
        token
        for token in re.split(r"[^a-z0-9_:.]+", normalized)
        if len(token) > 1 and not re.fullmatch(r"cq\d+", token)
    ]


def citation_match_score(phrase: Any, snippet: Any) -> float:
    phrase_norm = normalize_citation_text(phrase)
    snippet_norm = normalize_citation_text(snippet)
    if not phrase_norm or not snippet_norm:
        return 0.0
    if phrase_norm in snippet_norm:
        return 1.0

    phrase_compact = re.sub(r"\s+", "", phrase_norm)
    snippet_compact = re.sub(r"\s+", "", snippet_norm)
    if phrase_compact and phrase_compact in snippet_compact:
        return 1.0

    phrase_tokens = citation_tokens(phrase)
    if not phrase_tokens:
        return 0.0
    snippet_tokens = set(citation_tokens(snippet))
    if not snippet_tokens:
        return 0.0
    return sum(1 for token in phrase_tokens if token in snippet_tokens) / len(phrase_tokens)


def best_evidence_match(phrase_text: Any, evidence: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], float, bool]:
    scored = [
        (citation_match_score(phrase_text, item.get("snippet")), item)
        for item in evidence
        if isinstance(item, dict)
    ]
    if not scored:
        return None, 0.0, False
    scored.sort(key=lambda row: row[0], reverse=True)
    best_score, best_item = scored[0]
    tied = len(scored) > 1 and best_score - scored[1][0] < REPAIR_TIE_MARGIN
    return best_item, best_score, tied


def validate_and_repair_citations(output: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    evidence = [item for item in (payload.get("evidence") or []) if isinstance(item, dict)]
    evidence_by_id = {str(item.get("evidence_id") or ""): item for item in evidence}
    repairs: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    id_repairs: Dict[str, str] = {}

    for idx, phrase in enumerate(output.get("ranked_evidence_phrases") or []):
        if not isinstance(phrase, dict):
            continue
        phrase_text = phrase.get("text") or ""
        cited_id = str(phrase.get("evidence_id") or "")
        cited = evidence_by_id.get(cited_id)
        cited_score = citation_match_score(phrase_text, cited.get("snippet") if cited else "")
        best_item, best_score, tied = best_evidence_match(phrase_text, evidence)
        best_id = str(best_item.get("evidence_id") or "") if best_item else ""

        if cited is None:
            warnings.append(
                {
                    "rank": phrase.get("rank") or idx + 1,
                    "type": "missing_evidence_id",
                    "evidence_id": cited_id,
                    "text": phrase_text,
                }
            )
        elif phrase.get("source_type") and phrase.get("source_type") != cited.get("type"):
            old_type = phrase.get("source_type")
            phrase["source_type"] = cited.get("type") or old_type
            repairs.append(
                {
                    "rank": phrase.get("rank") or idx + 1,
                    "type": "source_type_repaired",
                    "evidence_id": cited_id,
                    "from": old_type,
                    "to": phrase["source_type"],
                }
            )

        should_repair = (
            best_item is not None
            and best_id
            and best_id != cited_id
            and best_score >= REPAIR_MIN_SCORE
            and best_score - cited_score >= REPAIR_MIN_MARGIN
            and not tied
        )
        if should_repair:
            old_id = cited_id
            phrase["evidence_id"] = best_id
            phrase["source_type"] = best_item.get("type") or phrase.get("source_type") or ""
            id_repairs[old_id] = best_id
            repairs.append(
                {
                    "rank": phrase.get("rank") or idx + 1,
                    "type": "evidence_id_repaired",
                    "from": old_id,
                    "to": best_id,
                    "cited_score": round(cited_score, 3),
                    "best_score": round(best_score, 3),
                    "text": phrase_text,
                }
            )
        elif phrase.get("verbatim") is True and cited_score < REPAIR_MIN_SCORE:
            warnings.append(
                {
                    "rank": phrase.get("rank") or idx + 1,
                    "type": "weak_verbatim_match",
                    "evidence_id": cited_id,
                    "cited_score": round(cited_score, 3),
                    "best_evidence_id": best_id or None,
                    "best_score": round(best_score, 3),
                    "text": phrase_text,
                }
            )

    origin = output.get("nl_question_origin")
    if isinstance(origin, dict) and id_repairs:
        evidence_ids = origin.get("evidence_ids")
        if isinstance(evidence_ids, list):
            repaired_ids: List[str] = []
            for evidence_id in evidence_ids:
                repaired = id_repairs.get(str(evidence_id), str(evidence_id))
                if repaired not in repaired_ids:
                    repaired_ids.append(repaired)
            origin["evidence_ids"] = repaired_ids
        primary = origin.get("primary_evidence_id")
        if primary is not None and str(primary) in id_repairs:
            origin["primary_evidence_id"] = id_repairs[str(primary)]

    return {
        "repairs": repairs,
        "warnings": warnings,
        "repair_count": len(repairs),
        "warning_count": len(warnings),
    }


def build_system_prompt(
    base_prompt: str,
    schema: Dict[str, Any],
    examples_text: str,
) -> str:
    parts = [base_prompt.strip(), "\nOutput schema (JSON):", json.dumps(schema, ensure_ascii=False, indent=2)]
    if examples_text:
        parts.extend(["\nFew-shot examples:", examples_text])
    return "\n".join(parts)


def build_request_config(
    *,
    args: argparse.Namespace,
    prompt_hash: str,
    schema_hash: str,
    examples_hash: str,
    system_prompt_hash: str,
    api_key_env: Optional[str],
    base_url: Optional[str],
) -> Dict[str, Any]:
    generation_parameters = {
        "temperature": args.temperature,
        "top_p": args.top_p,
        "max_output_tokens": args.max_output_tokens or None,
        "reasoning_effort": args.reasoning_effort or None,
    }
    return {
        "script_version": SCRIPT_VERSION,
        "api_method": args.api_method,
        "requested_model": args.model,
        "api_key_env": api_key_env,
        "base_url": base_url,
        "timeout_s": args.timeout_s,
        "max_records": args.max_records,
        "input_path": args.input,
        "prompt_path": args.prompt,
        "schema_path": args.schema,
        "examples_path": args.examples or None,
        "prompt_hash": prompt_hash,
        "schema_hash": schema_hash,
        "examples_hash": examples_hash,
        "system_prompt_hash": system_prompt_hash,
        "generation_parameters": generation_parameters,
    }


def build_response_create_kwargs(args: argparse.Namespace) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {"model": args.model}
    if args.temperature is not None:
        kwargs["temperature"] = args.temperature
    if args.top_p is not None:
        kwargs["top_p"] = args.top_p
    if args.max_output_tokens:
        kwargs["max_output_tokens"] = args.max_output_tokens
    if args.reasoning_effort:
        kwargs["reasoning"] = {"effort": args.reasoning_effort}
    return kwargs


def build_chat_completion_kwargs(args: argparse.Namespace) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {"model": args.model}
    if args.temperature is not None:
        kwargs["temperature"] = args.temperature
    if args.top_p is not None:
        kwargs["top_p"] = args.top_p
    if args.max_output_tokens:
        kwargs["max_tokens"] = args.max_output_tokens
    if args.reasoning_effort:
        kwargs["reasoning_effort"] = args.reasoning_effort
    return kwargs


def resolve_client_config(args: argparse.Namespace) -> Tuple[Dict[str, Any], Optional[str], Optional[str]]:
    api_key_env = args.api_key_env or None
    api_key = os.getenv(api_key_env) if api_key_env else None
    base_url = args.base_url or (os.getenv(args.base_url_env) if args.base_url_env else None)
    client_kwargs: Dict[str, Any] = {"timeout": args.timeout_s}
    if api_key:
        client_kwargs["api_key"] = api_key
    if base_url:
        client_kwargs["base_url"] = base_url
    return client_kwargs, api_key_env if api_key else None, base_url


def run_model_request(
    *,
    client: Any,
    args: argparse.Namespace,
    response_create_kwargs: Dict[str, Any],
    chat_completion_kwargs: Dict[str, Any],
    system_prompt: str,
    user_prompt: str,
) -> Tuple[str, Dict[str, Any]]:
    if args.api_method == "responses.create":
        resp = client.responses.create(
            **response_create_kwargs,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return (resp.output_text or "").strip(), {
            "id": getattr(resp, "id", None),
            "model": getattr(resp, "model", None),
        }

    resp = client.chat.completions.create(
        **chat_completion_kwargs,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    choice = resp.choices[0] if getattr(resp, "choices", None) else None
    message = getattr(choice, "message", None)
    content = getattr(message, "content", "") if message is not None else ""
    return str(content or "").strip(), {
        "id": getattr(resp, "id", None),
        "model": getattr(resp, "model", None),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run NL generation with OpenAI over LLM inputs JSONL.")
    parser.add_argument("--input", default="var/llm/inputs.jsonl")
    parser.add_argument("--prompt", default="prompts/llm_nl_generation.prompt.txt")
    parser.add_argument("--schema", default="schemas/llm_output.schema.json")
    parser.add_argument("--examples", default="prompts/llm_nl_generation.examples.jsonl")
    parser.add_argument("--output", default="var/llm/outputs.jsonl")
    parser.add_argument("--errors", default="var/llm/outputs.errors.jsonl")
    parser.add_argument("--model", default="gpt-5")
    parser.add_argument(
        "--api-method",
        default="responses.create",
        choices=["responses.create", "chat.completions.create"],
        help="Use chat.completions.create for LiteLLM deployments that do not support the Responses API.",
    )
    parser.add_argument(
        "--api-key-env",
        default="GRAPHIA_API_KEY",
        help="Environment variable containing the API key. The key value is never written to outputs.",
    )
    parser.add_argument(
        "--base-url-env",
        default="GRAPHIA_BASE_URL",
        help="Environment variable containing the OpenAI-compatible base URL.",
    )
    parser.add_argument(
        "--base-url",
        default="",
        help="OpenAI-compatible base URL override. Prefer GRAPHIA_BASE_URL for normal use.",
    )
    parser.add_argument("--max-records", type=int, default=0, help="0 means all")
    parser.add_argument("--timeout-s", type=float, default=180.0, help="Per-request OpenAI timeout in seconds.")
    parser.add_argument("--temperature", type=float, default=None)
    parser.add_argument("--top-p", type=float, default=None)
    parser.add_argument("--max-output-tokens", type=int, default=0)
    parser.add_argument("--reasoning-effort", default="", choices=["", "minimal", "low", "medium", "high", "xhigh"])
    args = parser.parse_args()

    input_path = Path(args.input)
    prompt_path = Path(args.prompt)
    schema_path = Path(args.schema)
    examples_path = Path(args.examples) if args.examples else None
    out_path = Path(args.output)
    err_path = Path(args.errors)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    err_path.parent.mkdir(parents=True, exist_ok=True)

    inputs = load_jsonl(input_path)
    if args.max_records > 0:
        inputs = inputs[: args.max_records]
    base_prompt = prompt_path.read_text(encoding="utf-8")
    schema = load_json(schema_path)
    examples_text = load_examples(examples_path)
    system_prompt = build_system_prompt(base_prompt, schema, examples_text)
    prompt_hash = sha256_text(base_prompt)
    schema_hash = sha256_json(schema)
    examples_hash = sha256_text(examples_text)
    system_prompt_hash = sha256_text(system_prompt)
    client_kwargs, resolved_api_key_env, resolved_base_url = resolve_client_config(args)
    request_config = build_request_config(
        args=args,
        prompt_hash=prompt_hash,
        schema_hash=schema_hash,
        examples_hash=examples_hash,
        system_prompt_hash=system_prompt_hash,
        api_key_env=resolved_api_key_env,
        base_url=resolved_base_url,
    )
    request_config_hash = sha256_json(request_config)
    response_create_kwargs = build_response_create_kwargs(args)
    chat_completion_kwargs = build_chat_completion_kwargs(args)

    if OpenAI is None:
        raise RuntimeError("The openai package is required to run model generation.")
    client = OpenAI(**client_kwargs)
    ok_count = 0
    err_count = 0

    completed = load_completed(out_path)
    ensure_jsonl_file(err_path)
    with out_path.open("a", encoding="utf-8") as out_f, err_path.open("a", encoding="utf-8") as err_f:
        for idx, payload in enumerate(inputs, start=1):
            user_prompt = json.dumps(payload, ensure_ascii=False, indent=2)
            label = payload.get("query_label") or payload.get("query_id")
            kg_id = payload.get("kg_id")
            input_hash = sha256_json(payload)
            key = build_completion_key(
                payload.get("query_id"),
                payload.get("query_label"),
                payload.get("kg_id"),
                args.model,
                system_prompt_hash,
                input_hash,
                request_config_hash,
            )
            if key in completed:
                log(f"[{idx}/{len(inputs)}] skip {kg_id} {label} (already done)")
                continue
            log(f"[{idx}/{len(inputs)}] running {kg_id} {label}")
            started = time.time()
            try:
                text, response_metadata = run_model_request(
                    client=client,
                    args=args,
                    response_create_kwargs=response_create_kwargs,
                    chat_completion_kwargs=chat_completion_kwargs,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                )
                parsed = extract_first_json_object(text)
                if parsed is None:
                    raise ValueError("No JSON object found in model output")
                citation_validation = validate_and_repair_citations(parsed, payload)
                valid, validation_error = validate_output(parsed, schema)
                if not valid:
                    raise ValueError(f"Schema validation failed: {validation_error}")
                out_rec = {
                    "query_id": payload.get("query_id"),
                    "query_label": payload.get("query_label"),
                    "kg_id": payload.get("kg_id"),
                    "llm_output": parsed,
                    "model": args.model,
                    "run_signature": {
                        "model": args.model,
                        "prompt_hash": prompt_hash,
                        "schema_hash": schema_hash,
                        "examples_hash": examples_hash,
                        "system_prompt_hash": system_prompt_hash,
                        "input_hash": input_hash,
                        "request_config_hash": request_config_hash,
                    },
                    "request_config": request_config,
                    "response_metadata": response_metadata,
                    "citation_validation": citation_validation,
                    "elapsed_ms": int((time.time() - started) * 1000),
                    "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                }
                out_f.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                ok_count += 1
                log(f"[{idx}/{len(inputs)}] ok {payload.get('query_label')} ({out_rec['elapsed_ms']} ms)")
            except Exception as e:
                elapsed_ms = int((time.time() - started) * 1000)
                err_rec = {
                    "query_id": payload.get("query_id"),
                    "query_label": payload.get("query_label"),
                    "kg_id": payload.get("kg_id"),
                    "error": str(e),
                    "run_signature": {
                        "model": args.model,
                        "prompt_hash": prompt_hash,
                        "schema_hash": schema_hash,
                        "examples_hash": examples_hash,
                        "system_prompt_hash": system_prompt_hash,
                        "input_hash": input_hash,
                        "request_config_hash": request_config_hash,
                    },
                    "request_config": request_config,
                    "elapsed_ms": elapsed_ms,
                }
                err_f.write(json.dumps(err_rec, ensure_ascii=False) + "\n")
                err_count += 1
                log(f"[{idx}/{len(inputs)}] error {payload.get('query_label')} ({elapsed_ms} ms): {e}")

    log(f"Wrote {ok_count} outputs to {out_path.resolve()}")
    if err_count:
        log(f"Wrote {err_count} errors to {err_path.resolve()}")


if __name__ == "__main__":
    main()
