#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
output_dir="$repo_root/var/source-discovery"
env_file="${MUSPARQL_DISCOVERY_ENV_FILE:-$repo_root/.env}"

# Keep API credentials out of this tracked script. Load them from the ignored
# env file when it exists; otherwise use values exported by the calling shell.
# The env file should contain ordinary Bash assignments, for example:
#
#   GITHUB_TOKEN='...'
#   OPENALEX_MAILTO='name@example.org'
#   BRAVE_API_KEY='...'
if [[ -f "$env_file" ]]; then
  # shellcheck disable=SC1090
  source "$env_file"
fi

# These are the optional variables supported by kg_source_discovery.py.
export GITHUB_TOKEN="${GITHUB_TOKEN:-}"
export OPENALEX_MAILTO="${OPENALEX_MAILTO:-}"
export BRAVE_API_KEY="${BRAVE_API_KEY:-}"

if [[ -z "$GITHUB_TOKEN" ]]; then
  printf 'Warning: GITHUB_TOKEN is unset; GitHub API rate limits will be lower.\n' >&2
fi
if [[ -z "$OPENALEX_MAILTO" ]]; then
  printf 'Warning: OPENALEX_MAILTO is unset; OpenAlex requests will be anonymous.\n' >&2
fi
if [[ -z "$BRAVE_API_KEY" ]]; then
  printf 'Warning: BRAVE_API_KEY is unset; Brave web discovery will be skipped.\n' >&2
fi

python="$repo_root/.venv/bin/python"
if [[ ! -x "$python" ]]; then
  printf 'Error: expected project Python at %s\n' "$python" >&2
  exit 1
fi

discover=(
  "$python"
  -m musparql.kg_source_discovery
)
export PYTHONPATH="$repo_root/src${PYTHONPATH:+:$PYTHONPATH}"

outputs=(
  alyra.json
  camera-dei-deputati.json
  cdec.json
  nfdi4culture.json
  europeana.json
)

# The discovery command deliberately refuses to overwrite reports. Check every
# destination before starting so an old file cannot leave this batch half-run.
for output in "${outputs[@]}"; do
  if [[ -e "$output_dir/$output" ]]; then
    printf 'Error: output already exists: %s\n' "$output_dir/$output" >&2
    printf 'Move or rename existing reports before rerunning this batch.\n' >&2
    exit 1
  fi
done

mkdir -p "$output_dir"

run_discovery() {
  local output_name="$1"
  shift

  printf '\nDiscovering sources for %s\n' "$2" >&2
  "${discover[@]}" "$@" --output "$output_dir/$output_name"
}

# Names, projects, and aliases are taken from
# docs/experiments/source-discovery-Quagga-KGs.txt. Informational URLs and
# endpoints are not CLI inputs; the web backend can rediscover and rank them.
run_discovery alyra.json \
  --name "Archaic Lyric Poetry Ontology" \
  --project "TALOS AI for SSH" \
  --alias "ALyrA" \
  --alias "Archaic Lyrical Agora"

run_discovery camera-dei-deputati.json \
  --name "Camera dei Deputati Knowledge Graph" \
  --project "Italian Chamber of Deputies" \
  --alias "dati.camera.it"

run_discovery cdec.json \
  --name "CDEC Knowledge Graph" \
  --project "Fondazione Centro di Documentazione Ebraica Contemporanea" \
  --alias "CDEC KG" \
  --alias "dati.cdec.it" \
  --alias "Open Memory Project"

run_discovery nfdi4culture.json \
  --name "Culture Knowledge Graph" \
  --project "NFDI4Culture" \
  --alias "NFDI4Culture Knowledge Graph" \
  --alias "NFDI4Culture KG"

run_discovery europeana.json \
  --name "Europeana Knowledge Graph" \
  --project "Europeana cultural heritage platform" \
  --alias "Europeana"

printf '\nSaved all discovery reports in %s\n' "$output_dir" >&2
