-- SEG Leads Generation Project: Supabase tables, migration 001
-- Already applied to the seg-leads-agent project on 2026-10-16. Kept for the repo history.
-- Migration 002 renames date_ran to date_run and adds the functions.

create extension if not exists citext;

create table if not exists public.token_usage (
  id           bigint generated always as identity primary key,
  date_ran     date not null default current_date,
  tokens_used  int  not null default 0
);

create table if not exists public.leads (
  id            bigint generated always as identity primary key,
  company_name  citext not null unique
);

alter table public.token_usage enable row level security;
alter table public.leads       enable row level security;
