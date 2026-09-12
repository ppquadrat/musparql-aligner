def source_groups($evidence):
  [
    $evidence[]
    | {
        source_id: (.source_id // null),
        source_url: (.source_url // .source_catalog_url // null),
        source_path: (.source_path // null),
        repo_commit: (if (.repo_commit // "") == "" then null else .repo_commit end),
        evidence_type: .type
      }
  ]
  | group_by([.source_id, .source_url, .source_path, .repo_commit])
  | map({
      source_id: .[0].source_id,
      source_url: .[0].source_url,
      source_path: .[0].source_path,
      repo_commit: .[0].repo_commit,
      evidence_types: (map(.evidence_type) | unique)
    });

def nl_sources($input; $output):
  [
    $output.llm_output.nl_question_origin.evidence_ids[] as $evidence_id
    | ($input.evidence[] | select(.evidence_id == $evidence_id)) as $evidence
    | ($output.llm_output.ranked_evidence_phrases[] | select(.evidence_id == $evidence_id)) as $phrase
    | {
        evidence_id: $evidence_id,
        evidence_type: $evidence.type,
        source_id: ($evidence.source_id // null),
        source_text: $phrase.text,
        source_url: ($evidence.source_url // null),
        source_path: ($evidence.source_path // null),
        verbatim: $phrase.verbatim
      }
  ];

[
  $inputs[] as $input
  | select($input.kg_id != "linkedmusic")
  | ($outputs[] | select(.query_id == $input.query_id)) as $output
  | ($ledger[] | select(.query_id == $input.query_id)) as $query
  | {
      kg_id: $input.kg_id,
      query_id: $input.query_id,
      query_label: $input.query_label,
      sparql: $input.sparql_clean,
      sparql_hash: $input.sparql_hash,
      sparql_sources: source_groups($query.evidence),
      nl: {
        text: $output.llm_output.nl_question,
        origin: $output.llm_output.nl_question_origin.mode,
        model: $output.model,
        needs_review: $output.llm_output.needs_review,
        sources: nl_sources($input; $output)
      }
    }
]
| sort_by(.kg_id, .query_label)
| group_by(.kg_id)
| {
    schema: "musparql.quagga-filter-candidates.v1",
    purpose: "Candidate NL-SPARQL pairs from the KG-discovery follow-up run, prepared for comparison with existing Quagga pairs.",
    source_run_id: ($ARGS.named.source_run_id // "2026-08-23-104601-minimax-m2-5"),
    notes: [
      "Exact SPARQL hashes are unique within this file.",
      "Semantic or structurally rewritten duplicates may still exist.",
      "NL sources preserve every retained citation, including partial evidence used by generated formulations."
    ],
    record_count: (map(length) | add),
    graphs: map({kg_id: .[0].kg_id, record_count: length, records: .})
  }
