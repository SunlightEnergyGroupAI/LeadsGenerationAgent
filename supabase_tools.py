"""Supabase tools for the SEG leads agent.

Thin wrappers over the database functions in supabase/002_functions.sql and 004_keepalive.sql, so the
agent's graph nodes never write SQL. Every function here is safe to call from a
LangGraph node or to wrap as an agent tool.

Environment variables (set as GitHub Actions secrets, never committed):
    SUPABASE_URL               Project URL, from Project Settings > API
    SUPABASE_SERVICE_ROLE_KEY  The secret service_role key

Install:  pip install supabase
"""

from __future__ import annotations

import os
from datetime import date
from functools import lru_cache
from typing import Any, Iterable

from supabase import Client, create_client


@lru_cache(maxsize=1)
def get_client() -> Client:
    """Return one shared Supabase client for the whole run."""
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)


def _rpc(name: str, params: dict[str, Any]) -> Any:
    return get_client().rpc(name, params).execute().data


# ---------------------------------------------------------------- duplicates

def is_company_seen(company_name: str) -> bool:
    """True if SEG has already sourced this company.

    Matching ignores capitals, punctuation and legal suffixes, so
    "Acme Plastics Inc." and "ACME plastics, LLC" count as the same company.
    """
    return bool(_rpc("is_company_seen", {"p_company_name": company_name}))


def filter_new_companies(company_names: Iterable[str]) -> list[str]:
    """Return only the companies not seen before, in one database call.

    Use this on a whole candidate list before spending any paid credits.
    Duplicates inside the list itself are also removed.
    """
    names = [n for n in company_names if n and n.strip()]
    if not names:
        return []
    rows = _rpc("filter_new_companies", {"p_company_names": names}) or []
    return [row["company_name"] for row in rows]


def claim_company(company_name: str) -> bool:
    """Record a company as sourced. True if it was new, False if already seen.

    Call this just before spending credits on a company. If it returns False,
    another run or worker already has it: skip the company.
    """
    return bool(_rpc("claim_company", {"p_company_name": company_name}))


# ------------------------------------------------------------ token tracking

def start_run(run_date: date | None = None) -> int:
    """Open a row in token_usage for this run and return its id."""
    params = {"p_date": run_date.isoformat()} if run_date else {}
    return int(_rpc("start_run", params))


def add_tokens(run_id: int, tokens: int) -> int:
    """Add tokens to the run's total and return the new total.

    Call after every model call so a crashed run still records what it used.
    """
    if tokens <= 0:
        return 0
    return int(_rpc("add_tokens", {"p_run_id": run_id, "p_tokens": int(tokens)}))


def tokens_from_response(message: Any) -> int:
    """Read total tokens from a LangChain chat model response, if reported."""
    usage = getattr(message, "usage_metadata", None) or {}
    total = usage.get("total_tokens")
    if total is None:
        total = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)
    return int(total or 0)


__all__ = [
    "get_client",
    "is_company_seen",
    "filter_new_companies",
    "claim_company",
    "start_run",
    "add_tokens",
    "tokens_from_response",
]
