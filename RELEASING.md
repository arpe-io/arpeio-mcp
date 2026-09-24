# Releasing arpeio-mcp

How to cut a new release. A single GitHub release publishes to **both** PyPI and
the Hugging Face Space — both are automated via GitHub Actions.

## TL;DR

```bash
# 1. Bump the version in all three places (see "Version bump" below)
#    - pyproject.toml
#    - server.json   (two occurrences)
#    - CHANGELOG.md   (prepend a new section)

# 2. Commit and push to main
git add -A
git commit -m "Bump version to X.Y.Z"
git push origin main

# 3. Create the GitHub release — this triggers everything
gh release create vX.Y.Z --title "vX.Y.Z" --notes "..."
```

That's it. The release event fans out to two workflows automatically.

## What the release triggers

```
gh release create vX.Y.Z   (event: release: published)
        │
        ├── .github/workflows/publish.yml   → builds the package, publishes to PyPI
        │                                      (OIDC trusted publishing, no stored token)
        │
        └── .github/workflows/deploy-hf.yml → pushes the released commit to the
                                               Hugging Face Space, which rebuilds
                                               its Docker container
```

- **PyPI** and **HF** build independently. HF builds from source via the
  `Dockerfile` (`pip install .`), so it does not wait on PyPI.
- Pushing to `main` alone does **not** release anything — the **GitHub release**
  is the trigger.

## Step by step

### 1. Version bump

Update the version string in **three** files (they must match):

| File | What to change |
|---|---|
| `pyproject.toml` | `version = "X.Y.Z"` |
| `server.json` | both `"version": "X.Y.Z"` occurrences |
| `CHANGELOG.md` | prepend a `## [X.Y.Z] - YYYY-MM-DD` section with Added / Changed / Notes |

Quick sanity check that everything is in sync:

```bash
grep -rn 'X\.Y\.Z' pyproject.toml server.json
grep -m1 '## \[' CHANGELOG.md
```

### 2. Verify before releasing

```bash
pip install -e ".[dev]" -q
python -m pytest -q          # all tests must pass
```

### 3. Commit and push

```bash
git add -A
git commit -m "Bump version to X.Y.Z"
git push origin main
```

### 4. Create the GitHub release

```bash
gh release create vX.Y.Z --title "vX.Y.Z" --notes "$(cat <<'EOF'
Short summary.

### Added
- ...

### Changed
- ...
EOF
)"
```

(Or use the GitHub UI: **Releases → Draft a new release**, tag `vX.Y.Z`, publish.)

### 5. Confirm

```bash
# PyPI publish
gh run list --workflow=publish.yml --limit 1
# HF Space deploy
gh run list --workflow=deploy-hf.yml --limit 1
```

- PyPI: the new version appears at https://pypi.org/project/arpeio-mcp/
  (`pip install arpeio-mcp` gets it).
- HF Space: build logs at
  https://huggingface.co/spaces/arpe-io/arpeio-mcp → **Logs**. Then:
  ```bash
  curl -s https://arpe-io-arpeio-mcp.hf.space/
  ```

## Prerequisites (one-time)

| Requirement | Where | Notes |
|---|---|---|
| PyPI trusted publisher | PyPI project settings + the `pypi` GitHub environment | Uses OIDC (`id-token: write`); no API token stored. Already configured. |
| `HF_TOKEN` repo secret | GitHub → Settings → Secrets and variables → Actions | Write-scoped Hugging Face token for the `arpe-io/arpeio-mcp` Space. Set with `gh secret set HF_TOKEN`. Required for `deploy-hf.yml`. |
| `git remote hf` (manual fallback only) | local clone | `git remote add hf git@hf.co:spaces/arpe-io/arpeio-mcp` |

## Manual fallback for the Hugging Face Space

If the `HF_TOKEN` secret is missing or the Space history has diverged, deploy the
Space by hand (requires SSH write access to the Space):

```bash
git push hf main
```

You can also re-run the auto-deploy without cutting a release:

```bash
gh workflow run deploy-hf.yml
```

See [`HF_DEPLOY.md`](HF_DEPLOY.md) for more detail on the Space setup.
