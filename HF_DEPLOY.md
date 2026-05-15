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

## Optional: auto-deploy on release

To remove the manual `git push hf main` step, add a GitHub Action that mirrors
to the Space on `release: published`. It would:

1. Check out the tagged commit.
2. Configure git with an `HF_TOKEN` repo secret
   (https://huggingface.co/settings/tokens, write scope on the Space).
3. `git push https://USER:$HF_TOKEN@huggingface.co/spaces/arpe-io/arpeio-mcp HEAD:main`.

Not currently wired up — the Space is updated manually after each release.
