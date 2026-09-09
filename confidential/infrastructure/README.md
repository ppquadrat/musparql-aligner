# Confidential infrastructure material

This directory is for local copies of infrastructure handovers and other
operational source material that may contain provider account identifiers,
restore endpoints, named administrative arrangements, or similar details that
should not be published with the repository.

Everything in this directory except this README is ignored by Git. Do not place
passwords, private SSH keys, SMTP credentials, backup encryption keys, or other
secrets here; use the approved password manager or server-side secret store.

The public-safe production boundary for agents is
[`docs/ICF_HOSTING_BOUNDARY.md`](../../docs/ICF_HOSTING_BOUNDARY.md).
