# Xero Service v2 Layer System

## Goal

Define the boundary between deterministic ERP sync and agent-facing Xero tools.
The system has two layers:

| Layer | Name | Role |
|---|---|---|
| 1 | ERP connection API | Authoritative Xero REST sync, checkpointing, and warehouse feed |
| 2 | Xero MCP and agentic tools | Conversational/action tooling built on MCP and prompt patterns |

## Layer Map

```text
operator / scheduler / HTTP trigger
          |
          v
Layer 1: ERP connection API
  xero-auth      OAuth2 PKCE, refresh, token cache
  xero-client    Xero Accounting REST calls
  xero-state     Postgres checkpoints, tenants, run history
  xero-sync      fetch -> validate -> load -> checkpoint
  tooling/       Python BigQuery loader + normalizers
          |
          v
warehouse / downstream reporting

agent / assistant / MCP client
          |
          v
Layer 2: Xero MCP and agentic tools
  Xero MCP server       standardized tool interface to Xero
  Xero prompt library   implementation prompts and use-case specs
  Xero agent toolkit    ADK, LangChain, OpenAI agent examples
          |
          v
approved tool calls / business workflows
```

## Source Material

Layer 1 references:

- Xero OpenAPI: https://github.com/XeroAPI/Xero-OpenAPI
- Xero Python SDK: https://github.com/XeroAPI/xero-python
- xero-rs crate: https://docs.rs/xero-rs/latest/xero_rs/
- Existing v1 client: `../clients/xero_api_client.py`

Layer 2 references:

- Xero prompt library: https://github.com/XeroAPI/xero-prompt-library
- Xero MCP server: https://github.com/XeroAPI/xero-mcp-server
- Xero agent toolkit: https://github.com/XeroAPI/xero-agent-toolkit

## Invariants

1. Layer 1 owns persisted sync state: tenants, OAuth token fallback, checkpoints, and run history.
2. Layer 1 is the only layer allowed to advance warehouse checkpoints.
3. Layer 1 reads/writes Xero through tenant-scoped REST calls with explicit OAuth scopes.
4. Layer 2 exposes agent-friendly tools, but must not bypass Layer 1 rules for durable sync state.
5. Layer 2 write actions require explicit tool allowlists, scope checks, idempotency strategy, and audit logging.
6. All Xero calls must be traceable to tenant, operation, upstream endpoint/tool, and triggering actor.

## Boundary Rules

Layer 1 is for deterministic automation: scheduled sync, replay, reconciliation, and warehouse load.
Layer 2 is for guided workflows: natural-language operations, assistant-driven queries, drafts, and approved accounting actions.

When the same capability exists in both layers, use Layer 1 for production sync and Layer 2 for human-in-the-loop tooling.
