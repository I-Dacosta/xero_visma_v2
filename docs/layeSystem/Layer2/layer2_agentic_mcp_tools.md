# Layer 2 — Xero MCP and Agentic Tools

## Role

Layer 2 provides agent-facing access to Xero through MCP tools, prompt patterns, and framework examples. It is not the production sync engine. It is the controlled interface for assistants and users to query, draft, and execute approved Xero workflows.

## External References

| Source | Layer 2 Use |
|---|---|
| Xero MCP server | Standardized MCP bridge to Xero Accounting and Payroll actions |
| Xero prompt library | Reusable prompts/specs for building Xero integrations across languages and frameworks |
| Xero agent toolkit | Examples for ADK, LangChain, OpenAI Agents, and TypeScript/Python agent workflows using MCP |

## MCP Server Capabilities

The Xero MCP server exposes Xero operations as MCP commands. Fetched reference material shows support for read and write actions such as:

- list accounts, contacts, invoices, items, payments, tax rates, bank transactions
- list organisation details and financial reports
- create contacts, invoices, bank transactions, credit notes, items, payments, quotes
- update contacts, invoices, items, manual journals, quotes, tracking categories
- payroll-oriented commands where regional payroll access exists

## Authentication Modes

Layer 2 supports two MCP authentication styles:

| Mode | Use |
|---|---|
| Custom Connection | Good for local/dev or single-organisation tool runs |
| Bearer Token | Good when the host application owns OAuth/PKCE and passes runtime tokens into the MCP server |

The production preference for this project is bearer-token handoff from Layer 1 auth once multi-tenant runtime support is needed. Custom Connection can be useful for local prototypes and demos.

## Agentic Tool Boundary

Layer 2 tools must be treated as high-level business tools, not raw database access.

Every write-capable tool should define:

- tool name and business purpose
- input schema and validation rules
- required Xero OAuth scopes
- idempotency key or duplicate-prevention rule
- tenant and actor audit fields
- dry-run/preview behavior when practical
- clear error mapping for user-facing failures

## Prompt Library Role

The prompt library is a design input, not runtime code. Use it for:

- fast scaffold prompts for Xero workflows
- endpoint coverage discovery
- UI/API requirements examples
- testing and deployment checklist inspiration

Do not treat prompt output as authoritative until it is reviewed against Layer 1 REST contracts and Xero OpenAPI/SDK behavior.

## Agent Toolkit Role

The agent toolkit demonstrates how AI frameworks connect to Xero through the MCP server. For this service, use it as a reference for:

- agent framework adapters
- multi-step accounting workflows
- assistant-facing error handling
- safe separation between conversational interface and Xero operations

## Layer 2 Invariants

1. Agents do not hold long-lived secrets in prompts or memory.
2. Agent tools receive scoped credentials at runtime or use approved Custom Connection secrets in local-only contexts.
3. Agent write operations require explicit user/business approval unless a workflow is pre-authorized.
4. Agent outputs that affect accounting state must be auditable.
5. Layer 2 never updates Layer 1 checkpoints directly.
