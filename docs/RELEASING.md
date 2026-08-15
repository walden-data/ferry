# Releasing ferry-core

This document describes the secure, repeatable release process for the
`ferry-core` Python distribution. The distribution name on PyPI is
`ferry-core`; the Python import is `ferry` and the compiled extension is
`ferry._native`.

The release workflow lives in [`.github/workflows/release.yml`](../.github/workflows/release.yml).
It builds wheels and an sdist once, validates them, and publishes them
through GitHub Trusted Publishing (OIDC) — no long-lived API tokens are
stored in the repository.

## Supported targets

| Platform | Target | Wheel tag |
|----------|--------|-----------|
| Linux x86_64 | `x86_64-unknown-linux-gnu` | `manylinux_2_28_x86_64` |
| Linux aarch64 | `aarch64-unknown-linux-gnu` | `manylinux_2_28_aarch64` |
| macOS x86_64 | `x86_64-apple-darwin` | `macos_*_x86_64` |
| macOS arm64 | `aarch64-apple-darwin` | `macos_*_arm64` |
| Source | — | `sdist` |

Wheels are built per-CPython-version (3.9–3.14). Windows and musllinux
wheels are out of scope for this workflow.

## Version source of truth

The Cargo workspace version in [`Cargo.toml`](../Cargo.toml)
(`[workspace.package] version`) is the release source of truth. The
release workflow enforces that, on tag events, the Git tag (minus the
leading `v`) matches the Cargo version and the `ferry.__version__` string
in `crates/ferry-python/ferry/__init__.py`.

Versions are **immutable**: once a version is published to PyPI it cannot
be republished. To ship a fix, bump the version and cut a new tag.

## One-time setup

The following external controls are **mandatory prerequisites** before
any production release. Do not enable the PyPI Trusted Publisher until
all of them are in place.

### 1. GitHub environments

Create two environments in the repository settings
(**Settings → Environments**):

- `testpypi` — no required reviewer (used for manual dry runs).
- `pypi` — **required**: add a required independent reviewer, prevent
  self-review, and disable admin bypass (or limit it to a narrow
  release role). Configure the environment's **Deployment branches and
  tags** rule: select **Selected branches and tags**, then add a **Tag**
  rule with the pattern `v*`. Do **not** select "All tagged refs" —
  that would allow non-`v*` tags to trigger production deployments.
  Only `v*` tags must be permitted. This ensures deployments can only
  originate from release tags, not arbitrary branch pushes or
  unprotected tags. This is the production gate.

### 2. Trusted Publisher (PyPI)

Register `ferry-core` on PyPI as a Trusted Publisher via
**Account settings → Publishing → Add a new publisher → GitHub**:

| Field | Value |
|-------|-------|
| PyPI Project Name | `ferry-core` |
| Owner | `walden-data` |
| Repository name | `ferry` |
| Workflow filename | `release.yml` |
| Environment name | `pypi` |

### 3. Trusted Publisher (TestPyPI)

Register `ferry-core` on TestPyPI the same way, using environment name
`testpypi`. TestPyPI is a separate index; a PyPI registration does not
cover it.

### 4. Protected `main` branch and `v*` tag ruleset (mandatory)

**Protected `main` branch ruleset:** in **Settings → Rules → Rulesets**,
create a ruleset targeting the `main` branch that:

- Requires pull request reviews (at least one independent reviewer).
- Requires status checks to pass before merge.
- Prohibits direct pushes to `main` (all changes via reviewed PRs).
- Has no bypass by admins, or a tightly limited bypass.

**Protected `v*` tag ruleset:** create a second ruleset targeting tag
pattern `v*` that:

- Restricts who can create, update, or delete tags to a designated
  release role (e.g. maintainers).
- Prohibits tag updates and deletion (tags are immutable).
- Has no bypass by admins, or a tightly limited bypass.

Together these ensure that only reviewed code can reach `main`, and only
designated maintainers can tag a release. The workflow additionally
verifies at publish time that the tagged commit is an ancestor of
`origin/main` (see the ancestry check in `publish-pypi`). Tag protection
alone does not prove commit ancestry or prior code review — the
combination of branch protection, tag protection, and the workflow's
ancestry check closes this gap.

## Release procedure

### 1. Bump the version

Update the version in **both** places (the workflow guard requires them to
match):

- `Cargo.toml` → `[workspace.package] version`
- `crates/ferry-python/ferry/__init__.py` → `__version__`

Commit the bump on `main` and merge via a reviewed PR.

### 2. Optional TestPyPI dry run

Trigger the **Release** workflow manually
(**Actions → Release → Run workflow**) with `dry_run` unchecked. This
builds, validates, and publishes to TestPyPI only.

`ferry-core` currently has **no Python runtime dependencies**, so
verification from TestPyPI is safe and simple — install with `--no-deps`
to avoid any dependency-resolution against TestPyPI:

```bash
uv venv verify-env
source verify-env/bin/activate
uv pip install --no-deps \
  --index-url https://test.pypi.org/simple/ \
  ferry-core==<VERSION>
python -c "import ferry; import ferry._native; print(ferry.__version__)"
```

> **Future dependencies:** if `ferry-core` later gains Python runtime
> dependencies, do **not** use `--index-strategy unsafe-best-match` or
> `--extra-index-url` with TestPyPI — that allows any package on either
> index to shadow the intended source (dependency confusion). Instead,
> install `ferry-core` from TestPyPI with `--no-deps`, fetch its expected
> SHA-256 from the workflow logs, verify it, and install dependencies
> separately from PyPI with a lock file or explicit hashes.

### 3. Production release

Push a Git tag matching the Cargo version:

```bash
git tag v<VERSION>
git push origin v<VERSION>
```

The `v*` tag **push** triggers the workflow. A manual `workflow_dispatch`
against a tag ref cannot trigger production publication — every production
job also requires `github.event_name == 'push'`.

The `pypi` environment requires an independent reviewer to approve
publication. **Reviewers must inspect the tagged commit and the
validated artifact SHA-256 manifest (printed in the validate job logs)
before approval.**

After publication, the `verify-pypi` job installs the exact version from
PyPI and imports the package.

### 4. Verify

Confirm the version appears at
<https://pypi.org/project/ferry-core/#history> and that the
`verify-pypi` workflow job passed.

PEP 740 digital attestations are generated and uploaded automatically by
the `pypa/gh-action-pypi-publish` action using Sigstore. The
attestations cryptographically bind each distribution file's digest to
this repository, workflow, job, and commit via the OIDC identity.

Consumers and maintainers can inspect attestations through:

- **PyPI web UI**: each release file's page at
  <https://pypi.org/project/ferry-core/#files> shows a "Provenance"
  section with attestation details when attestations are present.
- **PyPI Integrity API**: provenance for a specific distribution file
  is available at
  `https://pypi.org/integrity/ferry-core/<VERSION>/<FILENAME>/provenance`,
  where `<FILENAME>` is the exact wheel or sdist filename. This returns
  a JSON provenance object containing attestation bundles and the
  Trusted Publishing identity. See the
  [PyPI Integrity API documentation](https://docs.pypi.org/api/integrity/).

If you need CLI-based attestation verification and the
`pypi-attestations` tooling (or an equivalent Sigstore `cosign`
verification flow) is available in your environment, use it to verify
the attestation signature against this repository's identity. Otherwise,
rely on the PyPI provenance UI or Integrity API above. Note that pip's
`--require-hashes` option verifies file hashes but does **not** verify
PEP 740 attestations or Trusted Publisher identity.

## Recovery

### Yank a broken release

Yanking is performed through the **PyPI web UI** (there is no CLI
subcommand for yanking):

1. Go to <https://pypi.org/manage/project/ferry-core/releases/>.
2. Find the version to yank.
3. Click **Yank**.

Yanking hides the version from default `pip install` (without a version
specifier) but keeps it downloadable for pinned installs.

### Patch release

Bump the version (patch segment), update both version locations, commit,
and cut a new `v*` tag. Never re-publish or force-push a tag for an
already-released version.

## TestPyPI caveats

- TestPyPI is a separate, less-trusted package index. Do not treat it as
  a namespace-equivalent mirror of PyPI.
- `ferry-core` has no Python runtime dependencies, so the safe
  verification path is `--no-deps` against `--index-url
  https://test.pypi.org/simple/` (see above).
- The workflow uses `skip-existing: true` on the TestPyPI publish action
  so re-running a manual dispatch does not fail on an already-uploaded
  version. Production PyPI does **not** use `skip-existing`; a duplicate
  version fails loudly, which is the correct behavior for an immutable
  release index.

## Security notes

- No PyPI API tokens are stored in GitHub secrets. Publication uses
  [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC).
- Default workflow permissions are `contents: read`. The `id-token: write`
  permission is granted only on the `publish-testpypi` and `publish-pypi`
  jobs (job-level, not workflow-level).
- All third-party GitHub Actions are pinned to immutable commit SHAs with
  human-readable version comments. Updates are deliberate, reviewed PRs.
- `uv` is pinned to an exact reviewed version via `setup-uv`.
- Artifacts are built once and promoted into publish jobs; publication
  never rebuilds.
- A SHA-256 manifest of the validated artifact set is produced after
  validation and verified in each publish job before upload,
  cryptographically binding the validated bytes to the published bytes.
- The `pypi` environment requires an independent human reviewer,
  restricts deployments to `v*` tags, and the workflow verifies the
  tagged commit is reachable from `origin/main`. Protected `main` and
  `v*` tag rulesets are mandatory.
- PEP 740 digital attestations are generated by the official
  `pypa/gh-action-pypi-publish` action and uploaded alongside the
  distribution files.