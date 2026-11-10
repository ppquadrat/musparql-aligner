#!/usr/bin/env python3
"""Fetch a round's submission receipts from the ICF server, then throw them away.

The receipts are the authoritative decision-to-pseudonym record: each one holds
which wording a reviewer or group proposed for each pair. They live on the ICF
server under the submission root, inside the daily encrypted backup, and the
database row for each submission carries its path and digest.

A receipt carries pseudonymous identifiers, the wordings proposed, and reviewer
comments. It carries no name, email, affiliation or profile field, and the
identity registry stays on the server, so a local working copy is pseudonymous
research data rather than the identity link.

The point of this script is that the local copies need not be backed up: the
server's copy is authoritative and inside the daily encrypted backup, and this
is how a round comes back. Keep a round while you are working on it; `--purge`
is for when you are done with it, or when a withdrawal has to be applied.

What it does not do: it never writes outside the round directory, it never
copies profiles or the identity registry, it runs only read-only commands on
the server, and it touches nothing to do with backups or their credentials.

Usage:
    # what is on the server for this round, digests only, no personal data
    python -m scripts.fetch_review_receipts --round ipl-2026-09-16 --manifest-only

    # fetch and verify
    python -m scripts.fetch_review_receipts --round ipl-2026-09-16

    # from a restored backup or any directory already on disk
    python -m scripts.fetch_review_receipts --round ipl-2026-09-16 \\
        --from-dir /mnt/restore/srv/musparql/var/review/submissions

    # when the build is done
    python -m scripts.fetch_review_receipts --round ipl-2026-09-16 --purge
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List

DEFAULT_HOST = "musparql.industrycommons.net"
DEFAULT_SUBMISSION_ROOT = "/srv/musparql/var/review/submissions"
DEFAULT_DATABASE = "/srv/musparql/var/musparql.sqlite3"
LOCAL_ROOT = Path("var/review/exports")
MANIFEST_NAME = "receipt-manifest.json"

# Read-only. Selects the submissions for one round's assignments, with the
# digest each receipt must match. No reviewer identity is selected: this script
# fetches evidence for a build and has no reason to pull the attribution out of
# the database as well.
MANIFEST_SQL = """
SELECT s.id, s.assignment_id, s.revision, s.export_path, s.export_digest,
       s.submitted_at, s.validation_status, s.inclusion_status
FROM review_submissions AS s
JOIN review_assignments AS a ON a.id = s.assignment_id
WHERE a.workshop_round_id = :round
ORDER BY s.assignment_id, s.revision;
"""


def run(cmd: List[str], *, dry_run: bool) -> str:
    if dry_run:
        print("  would run:", " ".join(cmd))
        return ""
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return "sha256:" + digest.hexdigest()


def remote_manifest(args: argparse.Namespace) -> List[Dict[str, Any]]:
    """Ask the server for the round's submission rows, read-only."""
    sql = MANIFEST_SQL.replace(":round", f"'{args.round}'")
    if "'" in args.round:
        raise ValueError("round identifiers do not contain quotes")
    remote = f"sqlite3 -readonly -json {args.database} \"{sql.strip()}\""
    out = run(["ssh", target(args), remote], dry_run=args.dry_run)
    if args.dry_run:
        return []
    return json.loads(out) if out.strip() else []


def target(args: argparse.Namespace) -> str:
    return f"{args.user}@{args.host}" if args.user else args.host


def local_round_dir(round_id: str) -> Path:
    return LOCAL_ROOT / round_id


def purge(round_id: str, *, dry_run: bool) -> int:
    directory = local_round_dir(round_id)
    if not directory.exists():
        print(f"Nothing to purge: {directory} does not exist.")
        return 0
    files = sorted(p for p in directory.iterdir() if p.is_file())
    print(f"{'Would remove' if dry_run else 'Removing'} {len(files)} files under {directory}")
    if dry_run:
        return 0
    shutil.rmtree(directory)
    print(
        "Purged. The authoritative copy stays on the ICF server and in its backup; "
        "re-fetch when a build needs it."
    )
    return 0


def fetch(args: argparse.Namespace) -> int:
    destination = local_round_dir(args.round)
    if args.from_dir:
        source = Path(args.from_dir)
        if not source.is_dir():
            raise ValueError(f"--from-dir is not a directory: {source}")
        manifest_path = source / MANIFEST_NAME
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else []
        if not manifest:
            print(
                f"No {MANIFEST_NAME} beside the receipts, so digests cannot be verified. "
                "Copying anyway and reporting each file's digest; verify them against the "
                "database before relying on the result."
            )
        candidates = sorted(p for p in source.glob("*.json") if p.name != MANIFEST_NAME)
    else:
        manifest = remote_manifest(args)
        candidates = []

    if args.manifest_only:
        print(json.dumps(manifest, indent=2))
        print(f"\n{len(manifest)} submissions recorded for round {args.round}.")
        return 0

    by_digest = {str(row.get("export_digest")): row for row in manifest}
    destination.mkdir(parents=True, exist_ok=True)
    if not args.from_dir:
        # One rsync of the round's receipts. --times keeps the server's
        # timestamps so a later diff is meaningful.
        remote_path = f"{target(args)}:{args.submission_root.rstrip('/')}/"
        run(
            ["rsync", "-az", "--times", "--include=*/", "--include=*.json", "--exclude=*",
             remote_path, str(destination) + "/"],
            dry_run=args.dry_run,
        )
        if args.dry_run:
            print(f"  would then verify each file against {len(manifest)} database digests")
            return 0
        candidates = sorted(p for p in destination.rglob("*.json") if p.name != MANIFEST_NAME)

    verified, unmatched = 0, []
    for path in candidates:
        digest = sha256_file(path)
        if args.from_dir and path.parent != destination:
            shutil.copy2(path, destination / path.name)
        if by_digest.get(digest):
            verified += 1
        else:
            unmatched.append((path.name, digest))

    if manifest:
        (destination / MANIFEST_NAME).write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    print(f"{verified} of {len(candidates)} receipts match a recorded database digest.")
    if unmatched:
        print(f"{len(unmatched)} did not match any recorded digest:")
        for name, digest in unmatched[:10]:
            print(f"  {name}  {digest}")
        print(
            "An unmatched receipt is not necessarily corrupt — it may belong to another round — "
            "but do not build from it without checking."
        )
    print(
        f"\nThe authoritative copy stays on the ICF server and in its backup, so these need no "
        f"backup of their own. Use --purge when you are finished with the round."
    )
    return 1 if (manifest and unmatched) else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--round", required=True, help="Workshop round identifier, e.g. ipl-2026-09-16.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--user", default="", help="SSH user; omit to use your SSH config.")
    parser.add_argument("--submission-root", default=DEFAULT_SUBMISSION_ROOT)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument(
        "--from-dir",
        default="",
        help="Read receipts from a directory already on disk, such as an isolated restore, "
        "instead of contacting the server.",
    )
    parser.add_argument(
        "--manifest-only",
        action="store_true",
        help="Print the round's submission rows and stop. Carries digests, not annotations.",
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="Delete this round's local receipts. Run it when the build is finished.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Say what would happen.")
    args = parser.parse_args()
    if "/" in args.round or args.round.startswith("."):
        raise ValueError("round must be a plain identifier, not a path")
    if args.purge:
        return purge(args.round, dry_run=args.dry_run)
    return fetch(args)


if __name__ == "__main__":
    raise SystemExit(main())
