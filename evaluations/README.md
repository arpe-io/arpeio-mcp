# arpeio-mcp evaluations

`arpeio_eval.xml` is a set of 10 independent, read-only, verifiable questions used
to check whether an LLM can drive the arpeio-mcp server to correct answers (Phase 4
of the MCP-builder workflow).

Each `<qa_pair>` has:
- a realistic `<question>` a user might ask, and
- a single stable `<answer>` verifiable by case-insensitive string comparison.

All answers are reachable through the server's read-only tools — typically:
- `fastbcp_info` / `fasttransfer_info` / `lakexpress_info` / `migratorxpress_info`
  with `action="parallelism" | "capabilities" | "version" | "workflow"`,
- `arpe_quick_start` (tool selection from a plain-English use case),
- `arpe_release_notes` and `search_docs`.

## How to run against an MCP client

Point any MCP client (Claude Desktop, the hosted HF Space, or a local
`arpeio-mcp` stdio process) at the server, then for each `<question>` let the model
answer using only the server's tools and compare (case-insensitive, trimmed)
against `<answer>`. A run "passes" a question when the model's final answer
contains the expected string.

No binaries are required — every answer comes from the command-builder /
capability-registry surface, which works without any Arpe.io binary installed.

## Keeping the answer key honest

`tests/test_evaluations.py` re-derives every ground-truth answer directly from the
server's source of truth (the parallelism recommender and the version registries)
and asserts it still matches the XML. If a future registry change moves an answer
(e.g. a parallelism default changes, or a capability lands in a different version),
that test fails — a signal to update both the registry and `arpeio_eval.xml`
together, rather than letting the eval drift.

Run it with:

```bash
python -m pytest tests/test_evaluations.py -q
```
