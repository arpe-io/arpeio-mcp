# Hugging Face Space deploy

The remote-hosted MCP at `https://arpe-io-arpeio-mcp.hf.space/sse` runs from a
separate Hugging Face Space repo. **It is not auto-synced from GitHub** — the
PyPI publish workflow (`.github/workflows/publish.yml`) only ships the package
to PyPI.

## How the Space is wired

| File at repo root | Role on HF |
|---|---|
| `Dockerfile` | Build instructions (Python 3.11-slim + `pip install .` + uvicorn/starlette) |
| `app.py` | Entry point: Starlette + SSE wrapper around `src.server.app`, listening on port 7860 |

HF auto-detects a Space as Docker when a `Dockerfile` is present and exposes
the container on `https://<owner>-<space>.hf.space`.

## How to update it

The Space is a second git remote on this clone:

```
hf	git@hf.co:spaces/arpe-io/arpeio-mcp
```

After a normal release (push to `origin`, tag, `gh release create`), publish to
the Space with:

```
git push hf main
```

That single push triggers a Space rebuild. Build progress and logs:
`https://huggingface.co/spaces/arpe-io/arpeio-mcp` → **Logs**.

## Verifying

After the build finishes (1-3 min):

```
curl -s https://arpe-io-arpeio-mcp.hf.space/health
curl -s https://arpe-io-arpeio-mcp.hf.space/debug | jq '.tools_count'
```

`/health` returns `{"status":"ok"}`; `/debug` exposes `tools_count` and
`tool_names` from `app.py`.

## If the remote is missing on a fresh clone

```
git remote add hf git@hf.co:spaces/arpe-io/arpeio-mcp
git fetch hf
```

You need write access to the `arpe-io/arpeio-mcp` Space on Hugging Face for
the push to succeed (SSH key registered at https://huggingface.co/settings/keys).

## Auto-deploy on release (wired up)

`.github/workflows/deploy-hf.yml` mirrors the released commit to the Space on
every published GitHub release, so a normal release now updates **both** PyPI and
the Space — no manual `git push hf main` needed.

**One-time setup:** add a write-scoped Hugging Face token as a repo secret named
`HF_TOKEN`:

1. Create the token at https://huggingface.co/settings/tokens (write access to the
   `arpe-io/arpeio-mcp` Space).
2. Add it at **GitHub repo → Settings → Secrets and variables → Actions → New
   repository secret**, name `HF_TOKEN`. Or from the CLI:
   ```
   gh secret set HF_TOKEN        # paste the token when prompted
   ```

The workflow can also be run on demand from the **Actions** tab
(*Deploy to Hugging Face Space* → *Run workflow*).

The manual `git push hf main` above still works and remains the fallback if the
token is missing or the Space history diverges (the workflow does a normal
fast-forward push and fails loudly rather than force-overwriting).
