# Governance

Hexeract is an Nubster open-source project. Its governance is
intentionally minimal so that decisions can be made quickly while the
project is still pre-stable.

## Roles

### Benevolent Dictator For Life (BDFL)

- **Pierrick Fonquerne**, Founder.

The BDFL has the final say on:

- The product vision and the scope boundaries declared in the
  [roadmap](../ROADMAP.md).
- Public API design decisions and semver classification of changes.
- Acceptance, rejection or deferral of contributions.
- Release cadence and the contents of each release.

Decisions are made with input from maintainers and the community, but the
BDFL retains tie-breaking authority. The intent is to keep the project
coherent while it finds its shape.

### Maintainers

Maintainers are contributors who have demonstrated sustained, high-quality
involvement and who are trusted with merge rights. Their responsibilities:

- Review and merge pull requests.
- Triage issues and shepherd discussions.
- Uphold the trunk-based development discipline described below.

New maintainers are nominated by an existing maintainer and confirmed by
the BDFL.

### Contributors

Anyone who opens an issue, comments on a discussion, or submits a pull
request. Contribution guidelines live in
[`CONTRIBUTING.md`](../CONTRIBUTING.md) and behaviour expectations live in
[`CODE_OF_CONDUCT.md`](../CODE_OF_CONDUCT.md).

## Development discipline

### Trunk-based development

- `main` is the single long-lived branch. It is always releasable.
- Work happens on short-lived branches named `feature/...`, `fix/...`,
  `chore/...`, `docs/...` or `refactor/...`, branched from `main`.
- Branches are expected to live for hours or days, not weeks. Long-lived
  branches accumulate drift and conflict; they are discouraged.

### Pull requests

- Every change to `main` lands through a pull request.
- A minimum of **one reviewer** (a maintainer other than the author) must
  approve before merge.
- The CI suite (`fmt`, `clippy`, `test`, `supply-chain`, `msrv-check`,
  `cargo-audit`) must be green before merge.
- Commits follow [Conventional Commits](https://www.conventionalcommits.org/).
- Merges to `main` are linear (rebase or squash). Merge commits are
  acceptable only when explicitly justified for traceability.

### Protected `main`

- No direct pushes to `main`.
- No force push to `main` under any circumstance. If `main` needs to move
  backwards, it is done through a revert pull request.

## Versioning

- Hexeract follows [Semantic Versioning](https://semver.org/) once it
  reaches `v1.0.0`. Pre-`v1.0.0` releases follow the conventions described
  in [`docs/SEMVER_POLICY.md`](SEMVER_POLICY.md).
- The minimum supported Rust version policy lives in
  [`docs/MSRV_POLICY.md`](MSRV_POLICY.md).
- Semver classification of a change (major, minor, patch) is proposed by
  the author, confirmed by the reviewer, and adjudicated by the BDFL when
  the impact is non-obvious.

## Changes to this document

Governance changes are themselves pull requests, with a `docs(governance):`
Conventional Commit, reviewed by a maintainer and approved by the BDFL.
