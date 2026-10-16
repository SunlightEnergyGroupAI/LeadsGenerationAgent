# Supabase layer for the SEG leads agent

Project: **seg-leads-agent** (SEG organization, US East). Live and tested on 2026-10-16.

## Tables
| Table | Columns | Purpose |
|---|---|---|
| `leads` | `id`, `company_name`, `company_key` (auto), `first_seen` | One row per company SEG has sourced. `company_key` is the normalized name and is unique, so "Acme Plastics Inc." and "ACME plastics, LLC" are the same company. |
| `token_usage` | `id`, `date_run`, `tokens_used` | One row per agent run. |
| `token_usage_monthly` (view) | `month`, `runs`, `tokens_used` | Totals for the cost case study. |

Both tables have row-level security on. Only the `service_role` key can read, write or call the functions.

## Functions the agent calls (`supabase_tools.py`, repo root next to `agent.py`)
| Python | Database function | What it does |
|---|---|---|
| `filter_new_companies(names)` | `filter_new_companies(text[])` | Returns only companies never seen before, one call per batch. Run before spending any paid credits. |
| `is_company_seen(name)` | `is_company_seen(text)` | True if SEG already sourced this company. |
| `claim_company(name)` | `claim_company(text)` | Records the company. True if new, False if already seen. Safe with parallel workers. |
| `start_run()` | `start_run(date)` | Opens the run's row in `token_usage`, returns its id. |
| `add_tokens(run_id, n)` | `add_tokens(bigint, int)` | Adds tokens after each model call, returns the running total. |
| `tokens_from_response(msg)` | (none) | Reads token counts from a LangChain response. |

## Suggested use in the graph
1. First node: `run_id = start_run()`.
2. After building the candidate list: `companies = filter_new_companies(candidates)`.
3. Before paid lookups for a company: `if not claim_company(name): skip`.
4. After every LLM call: `add_tokens(run_id, tokens_from_response(response))`.

## Setup
- For local runs, put the same two names in `.env` (already git-ignored) and call `load_dotenv()` before importing the tools.
- GitHub Actions secrets: `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` (Project Settings > API keys). Never commit them; the repo is public.
- `pip install supabase`
- Free plan projects pause after about a week without activity. `.github/workflows/supabase-keepalive.yml` calls the `keepalive()` function every Monday at 13:17 UTC. It uses the same two secrets and prints only pass or fail, never data.
- After committing it, open Actions > Supabase keep-alive > Run workflow once to confirm it passes.
- GitHub turns off scheduled workflows in a public repo after 60 days with no commits. The weekly agent work keeps the repo active; if it goes quiet, GitHub emails a warning and one click re-enables it.

## Files
- `001_tables.sql`: tables as first created (applied).
- `002_functions.sql`: rename to `date_run`, normalizer, functions, view, permissions (applied). Undo block at the end.
- `003_test.sql`: self-test. Last run 13 of 13 PASS.
- `004_keepalive.sql`: `keepalive()` health check, returns counts only (applied and tested).
- `../.github/workflows/supabase-keepalive.yml`: the weekly ping.
